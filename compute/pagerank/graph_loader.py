import redis
import os
import json
from tqdm import tqdm

# === 配置 ===
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
EDGE_FILE = "/app/data/edges.tsv"  # 确保 docker-compose 映射正确
BATCH_SIZE = 5000  # Redis Pipeline 批处理大小


def load_graph():
    """
    读取 edges.tsv，构建图结构，并初始化 PR 值。
    Redis 结构:
      - graph:nodes (List): 所有节点 ID
      - graph:out_links (Hash): node -> [target1, target2...]
      - graph:out_degree (Hash): node -> int
      - pr:ranks:current (Hash): node -> score
      - sys:node_count (String): N
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    print("🧹 Cleaning old graph data...")
    r.flushall()  # 清空整个数据库，确保干净开始

    print(f"🚀 Loading graph from {EDGE_FILE}...")

    # 临时缓存，用于构建邻接表
    # 注意：如果图极大，这里应该用流式处理或多次扫描。
    # 对于维基百科 simple 级别，内存 dict 足够。
    adj_list = {}
    all_nodes = set()

    with open(EDGE_FILE, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc="Reading Edges"):
            try:
                parts = line.strip().split('\t')
                if len(parts) < 2: continue
                u, v = parts[0], parts[1]

                if u not in adj_list: adj_list[u] = []
                adj_list[u].append(v)

                all_nodes.add(u)
                all_nodes.add(v)
            except ValueError:
                continue

    N = len(all_nodes)
    print(f"📊 Graph Stats: {N} Nodes.")

    # 初始化 PR 值 = 1/N
    init_score = 1.0 / N

    print("📦 Pushing data to Redis...")
    pipe = r.pipeline()
    count = 0

    # 存入节点列表 (List)
    nodes_list = list(all_nodes)
    # 分批写入 graph:nodes
    for i in range(0, len(nodes_list), BATCH_SIZE):
        pipe.rpush("graph:nodes", *nodes_list[i: i + BATCH_SIZE])

    # 遍历所有节点存结构
    for node in tqdm(nodes_list, desc="Saving Redis"):
        targets = adj_list.get(node, [])

        # 1. 存出链 (仅存有出链的)
        if targets:
            pipe.hset("graph:out_links", node, json.dumps(targets))
            pipe.hset("graph:out_degree", node, len(targets))
        else:
            # 悬挂节点: degree=0 (代码逻辑中 targets为空即为悬挂)
            pass

        # 2. 初始化分数
        pipe.hset("pr:ranks:current", node, init_score)

        count += 1
        if count % BATCH_SIZE == 0:
            pipe.execute()

    # 保存总节点数 N
    pipe.set("sys:node_count", N)
    pipe.execute()

    print("✅ Graph Loaded Successfully.")


if __name__ == "__main__":
    load_graph()