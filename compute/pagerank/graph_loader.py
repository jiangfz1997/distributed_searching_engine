import redis
import os
import json
from tqdm import tqdm

# load graph into Redis for PageRank computation


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
EDGE_FILE = "/app/data/edges.tsv"
BATCH_SIZE = 5000


def load_graph():

    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    print("Cleaning old graph data...")
    r.flushall()  # Clean all old data

    print(f"Loading graph from {EDGE_FILE}...")


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
    print(f"Graph Stats: {N} Nodes.")

    init_score = 1.0 / N

    print(" Pushing data to Redis...")
    pipe = r.pipeline()
    count = 0

    nodes_list = list(all_nodes)
    # print(f"Node number : {len(nodes_list)}")

    for i in range(0, len(nodes_list), BATCH_SIZE):
        pipe.rpush("graph:nodes", *nodes_list[i: i + BATCH_SIZE])


    for node in tqdm(nodes_list, desc="Saving Redis"):
        targets = adj_list.get(node, [])

        if targets:
            pipe.hset("graph:out_links", node, json.dumps(targets))
            pipe.hset("graph:out_degree", node, len(targets))
        else:
            # print(f"Dangling node found: {node}")
            pass

        pipe.hset("pr:ranks:current", node, init_score)

        count += 1

        if count % BATCH_SIZE == 0:
            pipe.execute()

    pipe.set("sys:node_count", N)
    pipe.execute()

    print("Graph Loaded Successfully.")


if __name__ == "__main__":
    load_graph()