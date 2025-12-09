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

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from compute.db_utils import get_db_connection

NUM_PARTITIONS = 16
DATA_DIR = "/app/data"
TEMP_DIR = os.path.join(DATA_DIR, "temp_shuffle")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
Q_SOURCE = 'queue:indexing:reducer'
Q_PROCESSING = 'queue:indexing:reducer:processing'
Q_DEAD = 'queue:indexing:reducer:dead'


def run_reducer_task(partition_id):

    print(f"[Reducer] Processing Partition {partition_id}...", flush=True)

    pattern = os.path.join(TEMP_DIR, f"part-task*-r{partition_id}.pkl")
    files = glob.glob(pattern)

    if not files:
        print(f"  No files found for partition {partition_id}, skipping.", flush=True)
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

        # K-way merge sorted iterators
        merged_stream = heapq.merge(*iterators, key=lambda x: x[0])

        sql = """
            INSERT INTO inverted_index (term, df, postings)
            VALUES (%s, %s, %s)
            ON CONFLICT (term) DO UPDATE 
            SET df = EXCLUDED.df, postings = EXCLUDED.postings;
        """

        batch_data = []
        BATCH_SIZE = 3000
        count_terms = 0

        for term, group in groupby(merged_stream, key=lambda x: x[0]):
            if len(term.encode('utf-8')) > 512: continue

            postings_map = {}
            for _, doc_id, tf in group:
                postings_map[doc_id] = postings_map.get(doc_id, 0) + tf

            df = len(postings_map)


            batch_data.append((term, df, Json(postings_map)))
            count_terms += 1

            if len(batch_data) >= BATCH_SIZE:
                cursor.executemany(sql, batch_data)
                batch_data = []

        if batch_data:
            cursor.executemany(sql, batch_data)

        conn.commit()
        print(f"[Reducer] Partition {partition_id} Done. ({count_terms} terms)", flush=True)

    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        for f in file_handles: f.close()
        if cursor: cursor.close()
        if conn: conn.close()

# If failed, retry or mark dead, send task back to redis
def handle_error(r, raw_task, partition_id, error_msg, retries):
    pipe = r.pipeline()
    pipe.lrem(Q_PROCESSING, 1, raw_task)

    if retries < 3:
        print(f"[Reducer] Partition {partition_id} failed ({retries + 1}/3). Retrying...", flush=True)
        new_task_data = {"id": partition_id, "retries": retries + 1}


        pipe.lpush(Q_SOURCE, json.dumps(new_task_data))
    else:
        print(f"[Reducer] Partition {partition_id} DIED. Reason: {error_msg}", flush=True)
        dead_msg = {"id": partition_id, "error": str(error_msg)}
        pipe.rpush(Q_DEAD, json.dumps(dead_msg))

    pipe.execute()


def run_worker():

    print(f"Connecting to Redis at {REDIS_HOST}...", flush=True)
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    except Exception as e:
        print(f"Redis connection failed: {e}", flush=True)
        return

    print("Reducer Worker Started. Waiting for partitions...", flush=True)


    MAX_IDLE = 5
    idle_count = 0

    while True:
        raw_task = r.brpoplpush(Q_SOURCE, Q_PROCESSING, timeout=2)

        if not raw_task:
            idle_count += 1
            if idle_count >= MAX_IDLE:

                print("Queue empty. Reducer exiting.", flush=True)
                break
            continue

        idle_count = 0

        partition_id = None
        retries = 0

        try:
            try:
                task_dict = json.loads(raw_task)
                if isinstance(task_dict, int):
                    partition_id = task_dict
                else:
                    partition_id = task_dict['id']
                    retries = task_dict.get('retries', 0)
            except:
                partition_id = int(raw_task)

            run_reducer_task(partition_id)

            r.lrem(Q_PROCESSING, 1, raw_task)

        except Exception as e:
            print(f"Worker Error processing {raw_task}: {e}", flush=True)


            if partition_id is not None:
                handle_error(r, raw_task, partition_id, str(e), retries)


            else:
                r.lrem(Q_PROCESSING, 1, raw_task)


if __name__ == "__main__":
    run_worker()