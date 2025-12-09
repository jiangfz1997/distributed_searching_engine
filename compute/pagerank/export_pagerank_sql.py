import redis
import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from compute.db_utils import get_db_connection


REDIS_HOST = os.getenv("REDIS_HOST", "redis")

# using this version to export PageRank to PostgreSQL
def export_pr_sql():
    print(f" Connecting to Redis at {REDIS_HOST}...")
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        if not r.exists("pr:ranks:current"):
            print("Error: No PageRank data found in Redis.")
            return
    except Exception as e:
        print(f" Redis connection failed: {e}")
        return

    print("Fetching PageRank from Redis...")
    raw_data = r.hgetall("pr:ranks:current")
    print(f" Total Nodes: {len(raw_data)}")

    print(f" Connecting to PostgreSQL...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f" Database connection failed: {e}")
        return

    print(" Truncating 'pagerank' table...")
    cur.execute("TRUNCATE TABLE pagerank;")


    print(" Inserting PageRank scores...")

    sql = """
        INSERT INTO pagerank (doc_id, score) 
        VALUES (%s, %s)
        ON CONFLICT (doc_id) DO UPDATE 
        SET score = EXCLUDED.score;
    """

    data_tuples = [(k, float(v)) for k, v in raw_data.items()]
    BATCH_SIZE = 10000

    for i in range(0, len(data_tuples), BATCH_SIZE):
        batch = data_tuples[i: i + BATCH_SIZE]
        cur.executemany(sql, batch)
        print(f"   Processed {min(i + BATCH_SIZE, len(data_tuples))}...", end='\r')

    conn.commit()

    print("\n === TOP 10 PAGES BY PAGERANK (FROM DB) ===")
    cur.execute("SELECT doc_id, score FROM pagerank ORDER BY score DESC LIMIT 10")
    for rank, (doc_id, score) in enumerate(cur.fetchall(), 1):
        print(f"{rank:<3} {score:.8f}  {doc_id}")

    cur.close()
    conn.close()
    print("\n PageRank export to PostgreSQL complete!")


if __name__ == "__main__":
    export_pr_sql()