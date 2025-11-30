import redis
import time
import math
import sys,os,csv

# === 配置 ===
REDIS_HOST = "redis"
TASK_BATCH_SIZE = 2000  # 每个任务包包含多少个节点
MAX_ITERATIONS = 100  # 迭代次数
DAMPING_FACTOR = 0.85
CONVERGENCE_THRESHOLD = 1e-06 # 收敛阈值 (总误差小于此值即停止)
LOG_FILE = "/app/log/output/pr_convergence.csv"

def generate_tasks(r, total_nodes):
    """生成任务包：分批写入 Redis 并显示进度"""

    # 1. 清理旧队列
    if r.exists("queue:pr:tasks"):
        # print("🧹 Clearing old task queue...")
        r.delete("queue:pr:tasks")

    pipe = r.pipeline()
    task_count = 0

    # 2. 计算总任务数（用于显示百分比）
    total_tasks = math.ceil(total_nodes / TASK_BATCH_SIZE)

    # 3. 分批生成
    # PIPELINE_CHUNK: 每积攒多少个任务提交一次 Redis (防止内存积压)
    PIPELINE_CHUNK = 1000

    print(f"📦 Generating {total_tasks} tasks (Batch Size: {TASK_BATCH_SIZE})...")

    for start in range(0, total_nodes, TASK_BATCH_SIZE):
        # 任务格式: "start_index,count"
        pipe.rpush("queue:pr:tasks", f"{start},{TASK_BATCH_SIZE}")
        task_count += 1

        # 每积累 1000 个任务，或者达到总数，就提交一次
        if task_count % PIPELINE_CHUNK == 0:
            pipe.execute()  # 真正写入 Redis
            # 打印进度
            percent = (task_count / total_tasks) * 100
            print(f"    - Generated {task_count}/{total_tasks} tasks ({percent:.1f}%)", end='\r')

    # 提交剩余的任务
    pipe.execute()
    print(f"✅ Generated {task_count} tasks in total.        ")  # 空格是为了覆盖上面的 \r

    return task_count


def wait_for_tasks(r, total_tasks):
    """
    等待所有任务被【完成】(ACK)，而不仅仅是被【领走】
    """
    print(f"    Waiting for {total_tasks} tasks to complete...", end='', flush=True)

    while True:
        # 获取已完成的任务数 (ACK)
        # 这里的 key 是 sys:phase_ack
        done_count = int(r.get("sys:phase_ack") or 0)

        # 打印进度条效果
        percent = (done_count / total_tasks) * 100
        print(f"\r    Waiting for {total_tasks} tasks... {percent:.1f}% ({done_count}/{total_tasks})", end='',
              flush=True)

        if done_count >= total_tasks:
            print("")  # 换行
            return

        time.sleep(0.2)  # 轮询间隔


def run_controller():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    print(f"📝 Logging convergence data to {LOG_FILE}...")
    print(f"CONVERGENCE_THRESHOLD = {CONVERGENCE_THRESHOLD}")
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    cleanup_state(r)
    # 检查图是否加载
    if not r.exists("sys:node_count"):
        print("❌ Graph not found! Run graph_loader.py first.")
        sys.exit(1)

    total_nodes = int(r.get("sys:node_count"))
    print(f"🚦 Controller Started. Nodes: {total_nodes}")

    with open(LOG_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow(['Round', 'Duration_Seconds', 'Diff_Value'])

    for round_id in range(1, MAX_ITERATIONS + 1):
        print(f"\n=== 🏁 ROUND {round_id} START ===")
        start_time = time.time()

        # ==========================================
        # PHASE 1: SCATTER (分发贡献 + 统计悬挂)
        # ==========================================
        print(" -> Phase 1: Scatter")

        # 1. 清理中间数据
        r.delete("pr:accumulated")  # 这一轮收到的信件箱
        r.delete("pr:dangling_sum")  # 悬挂节点总和
        r.set("sys:phase_ack", 0)
        # 2. 生成任务
        num_tasks = generate_tasks(r, total_nodes)

        # 3. 发送信号
        r.set("sys:signal", "SCATTER")

        # 4. 等待完成
        wait_for_tasks(r, num_tasks)

        # ==========================================
        # 中间计算: 准备 Base Value
        # ==========================================
        dangling_sum = float(r.hget("pr:dangling_sum", "total") or 0.0)
        # PageRank 公式:
        # PR(u) = (1-d)/N + d * (Sum_In_Links + Dangling_Sum / N)
        # 提取公因式 Base = (1-d + d * Dangling_Sum) / N
        base_value = (1.0 - DAMPING_FACTOR + (DAMPING_FACTOR * dangling_sum)) / total_nodes

        # 将 Base 存入 Redis 供 Worker 在 Phase 2 使用
        r.set("sys:base_value", base_value)
        print(f"    (Dangling Sum: {dangling_sum:.4f}, Base Value: {base_value:.8f})")

        # ==========================================
        # PHASE 2: COMPUTE (应用公式 + 写入结果)
        # ==========================================
        print(" -> Phase 2: Compute")
        r.set("sys:convergence_diff", 0.0)  # 重置本轮误差计数器
        # 1. 清理下一轮结果表
        r.delete("pr:ranks:next")
        r.set("sys:phase_ack", 0)
        # 2. 再次生成同样的任务 (让 Worker 遍历所有节点应用公式)
        num_tasks = generate_tasks(r, total_nodes)

        # 3. 发送信号
        r.set("sys:signal", "COMPUTE")

        # 4. 等待完成
        wait_for_tasks(r, num_tasks)
        # ================= Check Convergence =================
        total_diff = float(r.get("sys:convergence_diff") or 0.0)
        duration = time.time() - start_time
        print(f"    Round {round_id} Done. Time: {duration:.2f}s, Diff: {total_diff:.6f}")
        with open(LOG_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            # 这里的 diff 建议存高精度
            writer.writerow([round_id, round(duration, 4), f"{total_diff:.10f}"])
        if total_diff < CONVERGENCE_THRESHOLD:
            print(f"✨ Converged at Round {round_id}! (Diff {total_diff} < {CONVERGENCE_THRESHOLD})")
            break
        # ==========================================
        # SWAP (翻转)
        # ==========================================

        print(" -> Swapping current/next...")
        r.delete("pr:ranks:current")
        r.rename("pr:ranks:next", "pr:ranks:current")

        duration = time.time() - start_time
        print(f"✅ Round {round_id} Done in {duration:.2f}s")

    print("\n🎉 PageRank Completed.")
    r.set("sys:signal", "SHUTDOWN")


def cleanup_state(r):
    """
    启动前清理上一轮残留的运行时 Key，但【严格保留】图结构数据。
    """
    print("🧹 Cleaning runtime state (keeping graph data)...")

    # 这些是运行时的临时 Key，删了不会影响图结构
    keys_to_delete = [
        "queue:pr:tasks",  # 任务队列
        "sys:signal",  # 控制信号 (SCATTER/COMPUTE)
        "sys:phase_ack",  # 阶段完成计数器
        "sys:base_value",  # PageRank 基础值
        "sys:convergence_diff",  # 收敛误差
        "pr:accumulated",  # Scatter 阶段的累加池
        "pr:dangling_sum",  # 悬挂节点总和
        "pr:ranks:next"  # 下一轮分数的缓冲区
    ]

    r.delete(*keys_to_delete)

    # 【可选】关于 pr:ranks:current (当前分数)
    # 如果你保留它：PageRank 会基于上一次的结果继续算（热启动，收敛更快）。
    # 如果你删掉它：你需要在这里重新初始化所有节点为 1/N。
    #
    # 为了方便调试，我们这里【保留】它。
    # 如果你想强制重置分数，可以在命令行加个参数，或者手动重置。

    print("✨ Runtime state cleared. Ready to start.")
if __name__ == "__main__":
    run_controller()