import redis
import os

# 连接 Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)  # 如果在容器外跑，端口映射要对


def audit_system():
    print("🔍 Auditing Redis State...")

    # === 修正 1：Key 名字改成 graph:nodes ===
    # === 修正 2：使用 llen (List Length) 而不是 zcard ===
    try:
        all_nodes = r.llen("graph:nodes")
    except Exception:
        # 如果代码改了用 Set，尝试 scard；如果用 ZSet，尝试 zcard
        # 但根据截图，你现在必须用 llen
        all_nodes = 0

    print(f"   Total Nodes in Graph: {all_nodes}")

    if all_nodes == 0:
        print("   ❌ GRAPH IS EMPTY! (Or key name is wrong)")
        return

    # 同样的，检查分数时也要注意 Key 名字
    # 截图里有个 pr 文件夹，说明 key 可能是 pr:current
    # 请检查一下你的 worker 代码到底写的是 pr_current 还是 pr:current

    # 假设是 pr:current (根据你的 graph:nodes 推测)
    current_key = "pr:ranks:current"
    # 如果 redis 里没这个 key，试试 pr_current


    current_count = r.hlen(current_key)
    print(f"   Nodes with Scores ({current_key}): {current_count}")

    # 3. 检查能量守恒 (Total Mass)
    # PageRank 所有节点分数加起来应该等于 N (或者 1.0，取决于你的初始化)
    # 注意：这步比较慢，如果是几百万节点慎用，或者只采样
    print("   Calculating Total Mass (Sampling)...")

    # 随机拿 10 个值看看数量级
    random_key = r.hrandfield("pr:ranks:current", 10, withvalues=True)
    print(f"   Sample Scores: {random_key}")

    # 获取 Dangling Sum
    dangling_sum = float(r.get("pr:dangling_sum") or 0)
    print(f"   Current Dangling Sum: {dangling_sum}")


if __name__ == "__main__":
    audit_system()