import subprocess
import time
import sys
import os

# === ⚙️ 配置区域 ===
# 并发工人数量 (根据你的 CPU 核数调整)
NUM_MAPPERS = 4
NUM_PR_WORKERS = 4

# 超时设置 (秒)
TIMEOUT_MAPPER = 600  # 等待 Mapper 完成的最大时间
TIMEOUT_PR = 600  # 等待 PageRank 完成的最大时间


def log(msg):
    print(f"\n🚀 [PIPELINE] {msg}")


def run_cmd(args, description, ignore_error=False):
    """执行 Shell 命令并打印耗时"""
    print(f"   👉 Executing: {description}...")
    start_time = time.time()

    try:
        # Windows 下 shell=True 通常更稳定
        use_shell = True if os.name == 'nt' else False

        # 将列表转为字符串命令 (方便 Windows 处理)
        if isinstance(args, list):
            cmd_str = " ".join(args)
        else:
            cmd_str = args

        subprocess.run(cmd_str, check=True, shell=use_shell)

    except subprocess.CalledProcessError as e:
        if ignore_error:
            print(f"   ⚠️ Warning: {description} failed (Ignored).")
        else:
            print(f"   ❌ Error in step: {description}")
            print(f"   Command: {cmd_str}")
            sys.exit(1)

    duration = time.time() - start_time
    print(f"   ✅ Done ({duration:.2f}s).")


def wait_for_service(service_name, check_cmd, timeout=60):
    """等待某个服务准备就绪 (通过反复执行 check_cmd)"""
    print(f"   ⏳ Waiting for {service_name} to be ready...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            subprocess.run(check_cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"   ✅ {service_name} is ready.")
            return
        except:
            time.sleep(2)
            print(".", end="", flush=True)
    print(f"\n   ❌ Timeout waiting for {service_name}.")
    sys.exit(1)


def main():
    total_start = time.time()
    print("=" * 60)
    print("   🔎 INDUSTRIAL SEARCH ENGINE - AUTOMATION PIPELINE")
    print("=" * 60)

    # ---------------------------------------------------------
    # 1. 环境清理与初始化
    # ---------------------------------------------------------
    log("Step 1: Environment Cleanup & Init")
    run_cmd("docker-compose down --remove-orphans", "Stopping old containers")

    # 重建镜像 (确保 NLTK, psycopg2 等依赖最新)
    log("Step 1.1: Building Images (ensure dependencies)")
    run_cmd("docker-compose build", "Building Docker Images")

    # 启动基础设施
    run_cmd("docker-compose up -d redis postgres", "Starting Infrastructure")

    # 等待 Postgres 就绪
    wait_for_service("Postgres", "docker-compose exec postgres pg_isready -U admin")

    # === 关键：清空旧数据 (Drop Tables) ===
    # 这一步是为了确保表结构更新为 JSONB
    drop_sql = "DROP TABLE IF EXISTS inverted_index; DROP TABLE IF EXISTS pagerank; DROP TABLE IF EXISTS metadata;"
    run_cmd(f'docker-compose exec postgres psql -U admin -d search_engine -c "{drop_sql}"', "Dropping old tables")

    # 清空 Redis
    run_cmd("docker-compose exec redis redis-cli FLUSHALL", "Flushing Redis")

    # 初始化新表
    run_cmd("docker-compose run --rm compute-node python compute/db_utils.py", "Initializing DB Tables (JSONB)")

    # ---------------------------------------------------------
    # 2. 数据清洗 (Ingestion)
    # ---------------------------------------------------------
    log("Step 2: Data Ingestion (XML -> JSONL)")
    # 假设你的 ingestion 脚本在 src/run_ingestion.py
    run_cmd("docker-compose run --rm compute-node python src/run_ingestion.py", "Running Ingestion")

    # ---------------------------------------------------------
    # 3. 倒排索引 (Indexing)
    # ---------------------------------------------------------
    log("Step 3: Distributed Indexing")

    # 3.1 清理中间文件
    run_cmd("docker-compose run --rm compute-node rm -rf /app/data/temp_shuffle/*", "Cleaning temp files")

    # 3.2 发布任务
    run_cmd("docker-compose run --rm compute-node python compute/indexing/controller.py --phase map",
            "Publishing Map Tasks")

    # 3.3 启动 Mapper 集群
    print(f"   🚀 Launching {NUM_MAPPERS} Mappers...")
    # 使用 scale 启动多个 mapper
    # 注意：mapper 必须有 idle 自动退出机制，否则这里会一直运行
    # 为了脚本能继续，我们使用 detached mode (-d)
    subprocess.run(f"docker-compose up -d --scale compute-node={NUM_MAPPERS}", shell=True)
    # 这里的 compute-node 默认 command 是 tail -f，我们需要手动指定 command 运行 mapper
    # 修正：直接用 run -d 多次
    for i in range(NUM_MAPPERS):
        subprocess.run("docker-compose run -d compute-node python compute/indexing/mapper.py", shell=True)

    # 3.4 等待 Mapper 完成
    print("   ⏳ Waiting for Mappers to finish (Monitor via Docker PS)...")
    wait_start = time.time()
    while True:
        # 检查是否还有 mapper.py 进程在跑
        res = subprocess.run('docker ps -q --filter "ancestor=search-compute:v1"', shell=True, capture_output=True,
                             text=True)
        # 注意：这里有点 tricky，因为 controller 和 reducer 也是这个镜像。
        # 最好是检查日志或进程列表。
        # 简单方案：查看 Redis 队列长度
        res_q = subprocess.run('docker-compose exec redis redis-cli LLEN queue:indexing:mapper', shell=True,
                               capture_output=True, text=True)
        queue_len = int(res_q.stdout.strip())

        # 还需要检查 processing 队列
        res_p = subprocess.run('docker-compose exec redis redis-cli LLEN queue:indexing:mapper:processing', shell=True,
                               capture_output=True, text=True)
        proc_len = int(res_p.stdout.strip())

        if queue_len == 0 and proc_len == 0:
            print("\n   ✅ All Map tasks processed.")
            # 给一点时间让 Mapper 写盘退出
            time.sleep(5)
            break

        if time.time() - wait_start > TIMEOUT_MAPPER:
            print("   ❌ Mapper Timeout!")
            sys.exit(1)

        print(f"      Remaining Tasks: {queue_len} | Processing: {proc_len}   ", end='\r')
        time.sleep(2)

    # 3.5 运行 Reducer (入库)
    run_cmd("docker-compose run --rm compute-node python compute/indexing/reducer.py",
            "Running Reducer (Insert to Postgres)")

    # ---------------------------------------------------------
    # 4. 图计算 (PageRank)
    # ---------------------------------------------------------
    log("Step 4: PageRank Calculation")

    # 4.1 提取边
    run_cmd("docker-compose run --rm compute-node python compute/pagerank/extract_edges.py", "Extracting Edges")

    # 4.2 加载图
    run_cmd("docker-compose run --rm compute-node python compute/pagerank/graph_loader.py", "Loading Graph to Redis")

    # 4.3 启动集群
    print(f"   🚀 Starting PR Controller + {NUM_PR_WORKERS} Workers...")
    subprocess.run(f"docker-compose up -d pr-controller --scale pr-worker={NUM_PR_WORKERS}", shell=True)

    # 4.4 等待收敛 (监控 Controller 退出)
    print("   ⏳ Waiting for PageRank convergence...")
    wait_start = time.time()
    while True:
        # 检查 pr-controller 容器是否还在运行
        res = subprocess.run('docker ps -q -f "name=pr-controller"', shell=True, capture_output=True, text=True)
        if not res.stdout.strip():
            print("\n   ✅ PageRank Controller finished.")
            break

        if time.time() - wait_start > TIMEOUT_PR:
            print("   ❌ PageRank Timeout!")
            sys.exit(1)

        time.sleep(5)
        print("      Calculation in progress...", end='\r')

    # 4.5 导出 PR 结果 (自动导出可能已在 Controller 做过，但这里再跑一次确保万一)
    run_cmd("docker-compose run --rm compute-node python compute/pagerank/export_pagerank_sql.py",
            "Exporting PR to Postgres")

    # 停止 PR 集群
    run_cmd("docker-compose stop pr-controller pr-worker", "Stopping PR Cluster")

    # ---------------------------------------------------------
    # 5. 元数据 (Metadata)
    # ---------------------------------------------------------
    log("Step 5: Metadata Export")
    run_cmd("docker-compose run --rm compute-node python compute/export_metadata.py",
            "Exporting Text & Length to Postgres")

    # ---------------------------------------------------------
    # 6. 服务上线
    # ---------------------------------------------------------
    log("Step 6: Deploying Search Engine")
    run_cmd("docker-compose up -d backend", "Starting Backend Service")

    # ---------------------------------------------------------
    # 完成
    # ---------------------------------------------------------
    total_time = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"🎉 PIPELINE COMPLETED in {total_time / 60:.2f} minutes!")
    print("👉 Search API: http://localhost:8000/docs")
    print("=" * 60)


if __name__ == "__main__":
    main()