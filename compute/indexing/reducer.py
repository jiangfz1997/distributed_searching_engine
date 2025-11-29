import os
import pickle
import glob
import heapq
import sys
from itertools import groupby
from psycopg2.extras import Json  # 用于处理 JSONB

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from compute.db_utils import get_db_connection

NUM_PARTITIONS = 16
DATA_DIR = "/app/data"
TEMP_DIR = os.path.join(DATA_DIR, "temp_shuffle")


def run_reducer_task(partition_id):
    print(f"⚙️  [Reducer] Processing Partition {partition_id}...")

    pattern = os.path.join(TEMP_DIR, f"part-task*-r{partition_id}.pkl")
    files = glob.glob(pattern)

    if not files:
        print(f"   ⚠️ No files, skipping.")
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

        merged_stream = heapq.merge(*iterators, key=lambda x: x[0])

        # Postgres Upsert
        sql = """
            INSERT INTO inverted_index (term, df, postings)
            VALUES (%s, %s, %s)
            ON CONFLICT (term) DO UPDATE 
            SET df = EXCLUDED.df, postings = EXCLUDED.postings;
        """

        batch_data = []
        BATCH_SIZE = 3000  # JSONB 数据量大，Batch 稍微调小
        count_terms = 0

        # 流元素: (term, doc_id, tf)
        for term, group in groupby(merged_stream, key=lambda x: x[0]):

            # 过滤超长垃圾词
            if len(term.encode('utf-8')) > 512: continue

            # === 聚合逻辑 ===
            postings_map = {}
            for _, doc_id, tf in group:
                # 累加 TF (正常情况下每个 doc_id 只出现一次，但为了健壮性做累加)
                postings_map[doc_id] = postings_map.get(doc_id, 0) + tf

            df = len(postings_map)

            # 存入 Json 对象
            batch_data.append((term, df, Json(postings_map)))
            count_terms += 1

            if len(batch_data) >= BATCH_SIZE:
                cursor.executemany(sql, batch_data)
                batch_data = []
                print(f"   Indexed {count_terms} terms...", end='\r')

        if batch_data:
            cursor.executemany(sql, batch_data)

        conn.commit()
        print(f"\n✅ [Reducer] Partition {partition_id} Done. ({count_terms} terms)")

    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        for f in file_handles: f.close()
        if cursor: cursor.close()
        if conn: conn.close()


# 单机循环运行所有分区
def run_all_reducers():
    print("🚀 Starting Reducer Sequence (JSONB Mode)...")
    for i in range(NUM_PARTITIONS):
        try:
            run_reducer_task(i)
        except Exception as e:
            print(f"❌ Partition {i} Failed: {e}")


if __name__ == "__main__":
    run_all_reducers()