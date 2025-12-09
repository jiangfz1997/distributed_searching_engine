import os
import json
import pickle
import hashlib
import redis
import sys
from collections import Counter


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from compute.utils.tokenizer import analyzer


NUM_PARTITIONS = 16
DATA_DIR = "/app/data"
INPUT_FILE = os.path.join(DATA_DIR, "intermediate", "corpus.jsonl")
TEMP_DIR = os.path.join(DATA_DIR, "temp_shuffle")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
Q_SOURCE = 'queue:indexing:mapper'
Q_PROCESSING = 'queue:indexing:mapper:processing'


def ensure_dirs():
    os.makedirs(TEMP_DIR, exist_ok=True)


def process_task(task):
    ensure_dirs()
    task_id = task['task_id']
    start_offset = task['start_offset']
    read_bytes = task['read_bytes']

    buckets = [[] for _ in range(NUM_PARTITIONS)]

    with open(INPUT_FILE, 'rb') as f:
        f.seek(start_offset)
        chunk_data = f.read(read_bytes)

    lines = chunk_data.decode('utf-8', errors='ignore').strip().split('\n')
    doc_count = 0

    for line in lines:
        if not line.strip(): continue
        try:
            doc = json.loads(line)
            doc_id = doc['id']
            text = doc.get('text', '')


            tokens = analyzer.analyze(text)


            term_counts = Counter(tokens)
            # print(f"Term counts {term_counts}")
            for term, tf in term_counts.items():
                h = int(hashlib.md5(term.encode()).hexdigest(), 16)
                p_idx = h % NUM_PARTITIONS
                buckets[p_idx].append((term, doc_id, tf))

            doc_count += 1
        except json.JSONDecodeError:
            continue

    for p_idx in range(NUM_PARTITIONS):
        data = buckets[p_idx]
        if not data: continue

        data.sort(key=lambda x: x[0])

        filename = f"part-task{task_id}-r{p_idx}.pkl"
        path = os.path.join(TEMP_DIR, filename)

        with open(path, 'wb') as out_f:
            pickle.dump(data, out_f)

    print(f"[Mapper] Task {task_id} done. {doc_count} docs.")


def run_worker():
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    except:
        print("Redis connection failed.")
        return

    print(f"Mapper Worker Started.")
    MAX_IDLE = 5
    idle_count = 0

    while True:
        raw_task = r.brpoplpush(Q_SOURCE, Q_PROCESSING, timeout=2)
        if not raw_task:
            idle_count += 1
            if idle_count >= MAX_IDLE:
                print("Queue empty. Mapper exiting.")
                break
            continue

        idle_count = 0
        try:
            task = json.loads(raw_task)
            process_task(task)
            r.lrem(Q_PROCESSING, 1, raw_task)
        except Exception as e:
            print(f"Error: {e}")
            r.lrem(Q_PROCESSING, 1, raw_task)


if __name__ == "__main__":
    run_worker()