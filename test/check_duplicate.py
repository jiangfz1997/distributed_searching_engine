import redis

# 确保配置和 Worker 一致
r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def find_missing():
    print("🕵️ Starting Investigation...")

    # 1. 获取所有节点 ID (有序列表)
    print("   Fetching graph:nodes...")
    all_nodes_list = r.lrange("graph:nodes", 0, -1)

    # 2. 获取所有有分数的节点 (Hash Keys)
    print("   Fetching pr:ranks:current keys...")
    scored_nodes_keys = r.hkeys("pr:ranks:current")
    scored_nodes_set = set(scored_nodes_keys)

    print(f"   Total Nodes: {len(all_nodes_list)}")
    print(f"   Scored Nodes: {len(scored_nodes_set)}")

    # 3. 找出差异
    missing_nodes = []
    missing_indices = []

    print("   Scanning for missing nodes...")
    for idx, node_id in enumerate(all_nodes_list):
        if node_id not in scored_nodes_set:
            missing_nodes.append(node_id)
            missing_indices.append(idx)

            # 只要找到前 10 个就够分析了，不用打几千个
            if len(missing_nodes) < 5:
                print(f"   ❌ Found Missing: Index={idx}, ID='{node_id}'")

    count = len(missing_nodes)
    print(f"\n🚨 Total Missing: {count}")

    if count > 0:
        # 分析丢失的位置规律
        first_idx = missing_indices[0]
        last_idx = missing_indices[-1]
        print(f"   📍 Missing Range: Index {first_idx} to {last_idx}")

        if count == 2000:
            if first_idx < 2000:
                print("   👉 DIAGNOSIS: The FIRST 4 batches failed (Startup Issue).")
            elif last_idx > len(all_nodes_list) - 2500:
                print("   👉 DIAGNOSIS: The LAST 4 batches failed (Shutdown/Cleanup Issue).")
            else:
                print("   👉 DIAGNOSIS: Random batches in the middle (Data Corruption?).")
    else:
        print("   ✅ No missing nodes found. (Wait, then why did check_redis say 2000 missing?)")


if __name__ == "__main__":
    find_missing()