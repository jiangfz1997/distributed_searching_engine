# compute/controller.py
import redis
import os
import json
import argparse


REDIS_HOST = 'redis'
REDIS_PORT = 6379
DATA_DIR = "/app/data"
INPUT_FILE = os.path.join(DATA_DIR, "intermediate", "corpus.jsonl")


def reset_redis(r):
    queues = [
        'queue:indexing:mapper',
        'queue:indexing:mapper:processing',
        'queue:indexing:reducer',
        'queue:indexing:reducer:processing'
    ]
    r.delete(*queues)
    print(" [Indexing Controller] All queues (pending & processing) cleared.")


def publish_mapper_tasks(r, chunk_size=1000):

    print(f"Scanning {INPUT_FILE} to generate tasks...")
    if not os.path.exists(INPUT_FILE):
        print("File not found.")
        return

    tasks = []
    task_id = 0

    with open(INPUT_FILE, 'rb') as f:
        start_offset = 0
        lines_count = 0

        for line in f:
            # print(f"Line!!!!!!! {line}")
            lines_count += 1
            if lines_count >= chunk_size:
                print(f"Line count reaching chunk size {lines_count}")
                end_offset = f.tell()

                task = {
                    "task_id": task_id,
                    "start_offset": start_offset,
                    "read_bytes": end_offset - start_offset
                }
                r.rpush('queue:indexing:mapper', json.dumps(task))

                task_id += 1
                lines_count = 0
                start_offset = end_offset

        if lines_count > 0:
            end_offset = f.tell()
            task = {
                "task_id": task_id,
                "start_offset": start_offset,
                "read_bytes": end_offset - start_offset
            }
            r.rpush('queue:indexing:mapper', json.dumps(task))
            task_id += 1

    print(f"Published {task_id} Mapper tasks to 'queue:indexing:mapper'")


def publish_reducer_tasks(r, num_reducers):
    REAL_PARTITIONS = 16
    print(f"Publishing {REAL_PARTITIONS} Partition tasks...")

    for i in range(REAL_PARTITIONS):
        r.rpush('queue:indexing:reducer', str(i))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["map", "reduce", "all"], default="all")
    parser.add_argument("--reducers", type=int, default=4)
    parser.add_argument("--chunk_size", type=int, default=2000)
    args = parser.parse_args()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    if args.phase in ["map", "all"]:
        reset_redis(r)
        publish_mapper_tasks(r, args.chunk_size)

    if args.phase in ["reduce", "all"]:
        if args.phase == "reduce":
            r.delete('queue:indexing:reducer')
        publish_reducer_tasks(r, args.reducers)