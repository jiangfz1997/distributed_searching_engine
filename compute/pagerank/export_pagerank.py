import redis
import json
import os
import time
# Deprecated: use export_pagerank_sql.py to export to PostgreSQL instead, saved for possible future use
# Too slow for large datasets and too much memory usage for backend

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
OUTPUT_FILE = "/app/data/output/pagerank.json"
# Export PageRank from Redis to JSON file


def export_pr():
    print(f" Connecting to Redis at {REDIS_HOST}...")
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    if not r.exists("pr:ranks:current"):
        print(" Error: No PageRank data found (pr:ranks:current is empty).")
        return

    print(" Fetching all ranks from Redis (this might take a moment)...")
    raw_data = r.hgetall("pr:ranks:current")

    print(f" Total Nodes Fetched: {len(raw_data)}")

    print(" Sorting data to find Top Pages...")
    sorted_ranks = sorted(
        raw_data.items(),
        key=lambda item: float(item[1]),
        reverse=True
    )

    print("\n === TOP 20 PAGES BY PAGERANK ===")
    print(f"{'Rank':<5} {'Score':<15} {'Page ID'}")
    print("-" * 40)
    for i in range(min(20, len(sorted_ranks))):
        page_id, score = sorted_ranks[i]
        print(f"{i + 1:<5} {float(score):.8f}    {page_id}")
    print("-" * 40)


    print(f"\n Saving to {OUTPUT_FILE}...")


    export_dict = {k: float(v) for k, v in raw_data.items()}

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(export_dict, f)

    print(" Export Complete!")


if __name__ == "__main__":
    export_pr()