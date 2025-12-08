import os
import pickle
import glob
import heapq
import sys
import json
import redis
import time
from itertools import groupby
from psycopg2.extras import Json

# 引入 db_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from compute.db_utils import get_db_connection

# === 配置 ===
NUM_PARTITIONS = 16
DATA_DIR = "/app/data"
TEMP_DIR = os.path.join(DATA_DIR, "temp_shuffle")

# === Redis 配置 ===
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
Q_SOURCE = 'queue:indexing:reducer'
Q_PROCESSING = 'queue:indexing:reducer:processing'
Q_DEAD = 'queue:indexing:reducer:dead'


def run_reducer_task(partition_id):
    """
    核心业务逻辑：处理指定的分区文件，入库 Postgres
    """
    print(f"⚙️  [Reducer] Processing Partition {partition_id}...", flush=True)

    pattern = os.path.join(TEMP_DIR, f"part-task*-r{partition_id}.pkl")
    files = glob.glob(pattern)

    if not files:
        print(f"   ⚠️ No files found for partition {partition_id}, skipping.", flush=True)
        return

    conn = None
    cursor = None
    file_handles = []

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        iterators = []
        for fname in files:
            f = open(fname, 'rb')
            file_handles.append(f)
            iterators.append(pickle.load(f))

        # 多路归并
        merged_stream = heapq.merge(*iterators, key=lambda x: x[0])

        # Postgres JSONB Upsert
        sql = """
            INSERT INTO inverted_index (term, df, postings)
            VALUES (%s, %s, %s)
            ON CONFLICT (term) DO UPDATE 
            SET df = EXCLUDED.df, postings = EXCLUDED.postings;
        """
        doc_length_map = {}

        batch_data = []
        BATCH_SIZE = 3000
        count_terms = 0

        for term, group in groupby(merged_stream, key=lambda x: x[0]):
            if len(term.encode('utf-8')) > 512: continue

            # 聚合逻辑
            postings_map = {}
            for _, doc_id, tf in group:
                postings_map[doc_id] = postings_map.get(doc_id, 0) + tf
                doc_length_map[doc_id] = doc_length_map.get(doc_id, 0) + tf

            df = len(postings_map)

            # 使用 Json 包装器
            batch_data.append((term, df, Json(postings_map)))
            count_terms += 1

            if len(batch_data) >= BATCH_SIZE:
                cursor.executemany(sql, batch_data)
                batch_data = []

        if batch_data:
            cursor.executemany(sql, batch_data)

        doc_sql = """
                    INSERT INTO metadata (doc_id, length)
                    VALUES (%s, %s)
                    ON CONFLICT (doc_id) DO UPDATE
                    SET length = COALESCE(metadata.length, 0) + EXCLUDED.length;
                """

        doc_batch = [(doc_id, length) for doc_id, length in doc_length_map.items()]
        cursor.executemany(doc_sql, doc_batch)

        conn.commit()
        print(f"✅ [Reducer] Partition {partition_id} Done. ({count_terms} terms)", flush=True)

    except Exception as e:
        if conn: conn.rollback()
        raise e  # 抛出异常，让外层 Worker 处理重试逻辑
    finally:
        for f in file_handles: f.close()
        if cursor: cursor.close()
        if conn: conn.close()


def handle_error(r, raw_task, partition_id, error_msg, retries):
    """异常处理：重试机制"""
    pipe = r.pipeline()
    pipe.lrem(Q_PROCESSING, 1, raw_task)  # 先移除当前处理中的

    if retries < 3:
        print(f"⚠️ [Reducer] Partition {partition_id} failed ({retries + 1}/3). Retrying...", flush=True)
        # 重新打包，增加 retry 计数
        new_task_data = {"id": partition_id, "retries": retries + 1}
        # 塞回源队列头部，优先重试
        pipe.lpush(Q_SOURCE, json.dumps(new_task_data))
    else:
        print(f"💀 [Reducer] Partition {partition_id} DIED. Reason: {error_msg}", flush=True)
        dead_msg = {"id": partition_id, "error": str(error_msg)}
        pipe.rpush(Q_DEAD, json.dumps(dead_msg))

    pipe.execute()


def run_worker():
    """
    分布式 Worker 主循环
    """
    print(f"🔌 Connecting to Redis at {REDIS_HOST}...", flush=True)
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    except Exception as e:
        print(f"❌ Redis connection failed: {e}", flush=True)
        return

    print("👷 Reducer Worker Started. Waiting for partitions...", flush=True)

    # 空闲退出机制
    MAX_IDLE = 5
    idle_count = 0

    while True:
        # 可靠队列模式：从 Source 弹出并推入 Processing
        raw_task = r.brpoplpush(Q_SOURCE, Q_PROCESSING, timeout=2)

        if not raw_task:
            idle_count += 1
            if idle_count >= MAX_IDLE:
                print("👋 Queue empty. Reducer exiting.", flush=True)
                break
            continue

        idle_count = 0  # 重置空闲计数

        partition_id = None
        retries = 0

        try:
            # 解析任务：兼容纯数字 "0" 和 JSON '{"id":0, "retries":1}'
            try:
                task_dict = json.loads(raw_task)
                if isinstance(task_dict, int):
                    partition_id = task_dict
                else:
                    partition_id = task_dict['id']
                    retries = task_dict.get('retries', 0)
            except:
                partition_id = int(raw_task)

            # === 执行核心任务 ===
            run_reducer_task(partition_id)

            # === 成功：ACK (从 Processing 移除) ===
            r.lrem(Q_PROCESSING, 1, raw_task)

        except Exception as e:
            print(f"❌ Worker Error processing {raw_task}: {e}", flush=True)
            # 失败处理：重试或死信
            if partition_id is not None:
                handle_error(r, raw_task, partition_id, str(e), retries)
            else:
                # 无法解析的任务直接移除，防止死循环
                r.lrem(Q_PROCESSING, 1, raw_task)


if __name__ == "__main__":
    run_worker()