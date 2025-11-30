import networkx as nx
import psycopg2
import os
import sys
from scipy.stats import spearmanr

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from compute.db_utils import get_db_connection


def validate():
    print("🚀 Fetching edges from Redis/File (Or reconstructing from raw)...")
    # 为了方便，这里我们假设直接读取 edges.tsv 文件构建标准图
    # (确保 edges.tsv 是最新的)
    EDGE_FILE = "/app/data/edges.tsv"

    if not os.path.exists(EDGE_FILE):
        print("❌ edges.tsv not found.")
        return

    print("Building NetworkX graph...")
    G = nx.DiGraph()
    with open(EDGE_FILE, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 2: continue
            u, v = parts[0], parts[1]
            G.add_edge(u, v)

    print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")

    print("🧮 Running NetworkX PageRank (Ground Truth)...")
    # alpha 就是 damping factor (0.85)
    nx_scores = nx.pagerank(G, alpha=0.85, tol=1e-06)

    print("📥 Fetching Your Distributed Scores from Postgres...")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT doc_id, score FROM pagerank")
    my_scores_map = dict(cur.fetchall())
    conn.close()

    print("⚔️ Comparing Results...")

    # 对齐数据：找出两个集合的交集
    common_nodes = set(nx_scores.keys()) & set(my_scores_map.keys())
    print(f"Overlapping nodes: {len(common_nodes)}")

    if len(common_nodes) == 0:
        print("❌ No common nodes found! Check ID formatting.")
        return

    list_nx = []
    list_my = []

    for node in common_nodes:
        list_nx.append(nx_scores[node])
        list_my.append(my_scores_map[node])

    # 计算相关系数
    # 1.0 = 完美正相关 (排名完全一致)
    # 0.0 = 完全无关
    correlation, p_value = spearmanr(list_nx, list_my)

    print("\n" + "=" * 40)
    print(f"📊 CORRELATION REPORT")
    print("=" * 40)
    print(f"Spearman Correlation: {correlation:.6f}")
    print("-" * 40)

    if correlation > 0.9:
        print("✅ PERFECT MATCH! Your distributed algorithm is correct.")
    elif correlation > 0.8:
        print("⚠️ GOOD MATCH. Slight differences (maybe damping/dangling handling).")
    else:
        print("❌ POOR MATCH. Something is wrong with the algorithm.")


if __name__ == "__main__":
    validate()