# compute/controller.py
import redis
import os
import json
import argparse

# 配置
REDIS_HOST = 'redis'
REDIS_PORT = 6379
DATA_DIR = "/app/data"
INPUT_FILE = os.path.join(DATA_DIR, "intermediate", "corpus.jsonl")


def reset_redis(r):
    # 清理所有相关队列
    queues = [
        'queue:indexing:mapper',
        'queue:indexing:mapper:processing', # 新增
        'queue:indexing:reducer',
        'queue:indexing:reducer:processing' # 新增
    ]
    r.delete(*queues)
    print("🧹 [Indexing Controller] All queues (pending & processing) cleared.")


def publish_mapper_tasks(r, chunk_size=1000):
    """
    扫描文件，生成带有【字节偏移量】的任务
    """
    print(f"📦 Scanning {INPUT_FILE} to generate tasks...")
    if not os.path.exists(INPUT_FILE):
        print("❌ File not found.")
        return

    tasks = []
    task_id = 0

    with open(INPUT_FILE, 'rb') as f:  # 二进制模式读取，保证偏移量准确
        start_offset = 0
        lines_count = 0

        for line in f:
            lines_count += 1
            if lines_count >= chunk_size:
                # 记录当前指针位置
                end_offset = f.tell()

                # 生成任务包
                task = {
                    "task_id": task_id,
                    "start_offset": start_offset,
                    "read_bytes": end_offset - start_offset  # 只需要读这么多字节
                }
                r.rpush('queue:indexing:mapper', json.dumps(task))

                # 重置计数器
                task_id += 1
                lines_count = 0
                start_offset = end_offset

        # 处理剩余的最后一块
        if lines_count > 0:
            end_offset = f.tell()
            task = {
                "task_id": task_id,
                "start_offset": start_offset,
                "read_bytes": end_offset - start_offset
            }
            r.rpush('queue:indexing:mapper', json.dumps(task))
            task_id += 1

    print(f"🚀 Published {task_id} Mapper tasks to 'queue:indexing:mapper'")


def publish_reducer_tasks(r, num_reducers):  # 虽然参数名还没改，但逻辑如下
    # 强制覆盖为 16，或者在调用时传入 16
    REAL_PARTITIONS = 16
    print(f"⚙️  Publishing {REAL_PARTITIONS} Partition tasks...")

    for i in range(REAL_PARTITIONS):
        # 简单的发数字 ID 即可，Reducer 会兼容处理
        r.rpush('queue:indexing:reducer', str(i))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["map", "reduce", "all"], default="all")
    parser.add_argument("--reducers", type=int, default=4)
    parser.add_argument("--chunk_size", type=int, default=2000)
    args = parser.parse_args()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    if args.phase in ["map", "all"]:
        reset_redis(r)  # 只有 map 阶段才清空，因为 reduce 依赖 map 的结果
        publish_mapper_tasks(r, args.chunk_size)

    if args.phase in ["reduce", "all"]:
        # 注意：实际上通常等 Map 完了再发 Reduce 任务，这里为了演示方便一起发
        # 或者你可以分两次运行脚本
        if args.phase == "reduce":
            # 如果只发 reduce 任务，清理一下旧的 reduce 队列
            r.delete('queue:indexing:reducer')
        publish_reducer_tasks(r, args.reducers)