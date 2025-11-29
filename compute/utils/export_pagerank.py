import redis
import json
import os
import time

# === 配置 ===
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
OUTPUT_FILE = "/app/data/output/pagerank.json"


def export_pr():
    print(f"🔌 Connecting to Redis at {REDIS_HOST}...")
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    if not r.exists("pr:ranks:current"):
        print("❌ Error: No PageRank data found (pr:ranks:current is empty).")
        return

    print("📦 Fetching all ranks from Redis (this might take a moment)...")
    # 获取整个 Hash 表
    raw_data = r.hgetall("pr:ranks:current")

    print(f"📊 Total Nodes Fetched: {len(raw_data)}")

    # 转换为 float 并排序 (耗时操作，但在几百万量级下 Python 还能扛得住)
    print("🔄 Sorting data to find Top Pages...")
    sorted_ranks = sorted(
        raw_data.items(),
        key=lambda item: float(item[1]),
        reverse=True  # 降序，分高的在前
    )

    # === 验证环节：打印 Top 20 ===
    print("\n🏆 === TOP 20 PAGES BY PAGERANK ===")
    print(f"{'Rank':<5} {'Score':<15} {'Page ID'}")
    print("-" * 40)
    for i in range(min(20, len(sorted_ranks))):
        page_id, score = sorted_ranks[i]
        print(f"{i + 1:<5} {float(score):.8f}    {page_id}")
    print("-" * 40)

    # === 落盘环节：存为 JSON ===
    # 为什么存 JSON？因为 Search Service 启动时可以直接加载进内存 dict
    print(f"\n💾 Saving to {OUTPUT_FILE}...")

    # 为了减小体积，我们可以只存 Dict {id: score}
    # 或者直接存排好序的 List (取决于你后端怎么用)
    # 这里我们存 Dict，方便 O(1) 查询
    export_dict = {k: float(v) for k, v in raw_data.items()}

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(export_dict, f)

    print("✅ Export Complete!")


if __name__ == "__main__":
    export_pr()