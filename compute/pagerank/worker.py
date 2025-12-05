import redis
import json
import time
import os
import random
# === 配置 ===
REDIS_HOST = "redis"
DAMPING_FACTOR = 0.85


def retry_execute(pipe, max_retries=3, backoff=1):
    """
    尝试执行 Pipeline，如果遇到连接错误或超时则重试。
    """
    for attempt in range(max_retries):
        try:
            return pipe.execute()
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if attempt == max_retries - 1:
                print(f"❌ Pipeline failed after {max_retries} attempts: {e}")
                raise e  # 抛出异常，让 Worker 崩溃/重启，绝对不能吞掉异常！

            sleep_time = backoff * (2 ** attempt)  # 指数退避: 1s, 2s, 4s
            print(f"⚠️ Pipeline write failed ({e}), retrying in {sleep_time}s...")
            time.sleep(sleep_time)
            # 注意：Pipeline 对象在 execute 失败后通常保持原样，可以直接再次 execute
    return None


def run_worker():
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    worker_pid = os.getpid()
    print(f"👷 Worker {worker_pid} Ready. Waiting for signals...")
    start_delay = random.uniform(0, 2)
    time.sleep(start_delay)
    while True:
        # 1. 获取当前信号
        signal = r.get("sys:signal")

        if signal == "SHUTDOWN":
            print("👋 Shutdown signal received.")
            break

        if signal not in ["SCATTER", "COMPUTE"]:
            # Controller 还没准备好
            time.sleep(0.2)
            continue

        # 2. 抢任务 (Micro-batch)
        # LPOP: 非阻塞弹出。如果想更安全可用 BLPOP 或 RPOPLPUSH
        raw_task = r.lpop("queue:pr:tasks")

        if not raw_task:
            # 没任务了，休息等待下一阶段
            time.sleep(0.1)
            continue

        # 3. 解析任务 "start,count"
        try:
            start_idx, count = map(int, raw_task.split(','))
            end_idx = start_idx + count - 1

            # 获取具体的节点 ID 列表
            node_ids = r.lrange("graph:nodes", start_idx, end_idx)
            if not node_ids: continue

            # === 根据信号执行不同逻辑 ===

            if signal == "SCATTER":
                do_scatter(r, node_ids)

            elif signal == "COMPUTE":
                do_compute(r, node_ids)
            r.incr("sys:phase_ack")
        except Exception as e:
            print(f"❌ Error processing task {raw_task}: {e}")

            # 🔥🔥🔥 核心修复：把任务塞回队列头，让别人（或者自己等会儿）再做一次
            print(f"♻️ Retrying task {raw_task}...")
            r.lpush("queue:pr:tasks", raw_task)

            # 稍微睡一下，避开当前的故障风头
            time.sleep(1)
            # 生产环境应将任务塞回队列


def do_scatter(r, nodes):
    """
    Phase 1: 读取 Current PR -> 分发给邻居 (累加) -> 统计悬挂节点
    """
    print(" -> Phase 1: Scatter Nodes")
    pipe = r.pipeline()

    # 批量获取当前分数和出链信息
    # 技巧: 为了减少 IO，我们假设出链在 graph:out_links，分数在 pr:ranks:current
    # 由于 pipeline 只能按顺序返回，我们需要一一对应

    for node in nodes:
        pipe.hget("pr:ranks:current", node)
        pipe.hget("graph:out_links", node)

    results = pipe.execute()

    # 准备写入管道
    write_pipe = r.pipeline()
    dangling_sum_local = 0.0

    # results 是 [score1, links1, score2, links2 ...]
    for i in range(0, len(results), 2):
        score_str = results[i]
        links_str = results[i + 1]

        current_score = float(score_str) if score_str else 0.0

        if not links_str:
            # === 悬挂节点 ===
            # 没有出链，分数贡献给全局 dangling_sum
            dangling_sum_local += current_score
        else:
            # === 正常节点 ===
            targets = json.loads(links_str)
            out_degree = len(targets)
            if out_degree > 0:
                contribution = current_score / out_degree
                for target in targets:
                    # 使用 HINCRBYFLOAT 原子累加
                    write_pipe.hincrbyfloat("pr:accumulated", target, contribution)

    # 提交累加值
    if dangling_sum_local > 0:
        write_pipe.hincrbyfloat("pr:dangling_sum", "total", dangling_sum_local)

    retry_execute(write_pipe)
    print(f"Scatter done for nodes. Dangling Sum Local: {dangling_sum_local}")

def do_compute(r, nodes):
    """Phase 2: 计算新分数 + 计算收敛误差"""
    print(" -> Phase 2: Compute Nodes")
    base_val = float(r.get("sys:base_value") or 0.0)

    pipe = r.pipeline()
    for node in nodes:
        pipe.hget("pr:accumulated", node)  # 获取别人给我的总钱数
        pipe.hget("pr:ranks:current", node)  # 获取我上一轮的旧分数 (用于对比)
    results = pipe.execute()

    write_pipe = r.pipeline()
    local_diff_sum = 0.0

    for i, node in enumerate(nodes):
        # 索引 i*2 是 accumulated, i*2+1 是 old_score
        accum_val = float(results[i * 2] or 0.0)
        old_score = float(results[i * 2 + 1] or 0.0)

        # 核心公式
        new_score = base_val + (DAMPING_FACTOR * accum_val)

        # 记录新分数
        write_pipe.hset("pr:ranks:next", node, new_score)

        # 计算误差 diff
        local_diff_sum += abs(new_score - old_score)

    # 提交新分数
    retry_execute(write_pipe)

    # 提交误差统计 (用于 Controller 判断是否提前收敛)
    if local_diff_sum > 0:
        r.incrbyfloat("sys:convergence_diff", local_diff_sum)
    print(f"Compute done for nodes. Local Diff Sum: {local_diff_sum}")

if __name__ == "__main__":
    run_worker()