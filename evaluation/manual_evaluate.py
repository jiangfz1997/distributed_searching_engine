import os
import sys
import json
import math
from collections import defaultdict

import numpy as np
from tqdm import tqdm

# 根据你的项目结构调整
sys.path.append("/app")

from serving.search_engine import SearchEngine


# ===== 配置 =====
QRELS_PATH = "/app/evaluation/manual_qrels.json"
TOPK = 20          # 每个 query 从系统里取多少个候选
K_VALUES = [1, 3, 5, 10]  # 计算 @k 的位置，可以按需改


def load_manual_qrels(path: str):
    """
    从 JSON 文件加载手动标注的评测数据。

    期望格式:
    [
      {
        "query": "how photosynthesis works",
        "relevant": {
          "Photosynthesis": 3,
          "Chlorophyll": 2
        }
      },
      ...
    ]

    也兼容:
    "relevant": ["Photosynthesis", "Chlorophyll"]
    这种写法会被自动转换成二值相关性 1。
    """
    if not os.path.exists(path):
        print(f"❌ Qrels file not found: {path}")
        print("   请先创建一个 manual_qrels.json 并写入你的手动标注。")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    queries = []
    for item in data:
        query = item["query"]
        rel = item["relevant"]

        # 支持 list 或 dict 两种写法
        if isinstance(rel, list):
            rel_dict = {doc_id: 1 for doc_id in rel}
        else:
            rel_dict = dict(rel)

        queries.append((query, rel_dict))

    print(f"✅ Loaded {len(queries)} manually labeled queries.")
    return queries


def ndcg_at_k(ranked_doc_ids, rel_dict, k):
    """
    计算单个 query 的 NDCG@k。
    rel_dict: {doc_id: relevance_score}
    ranked_doc_ids: 系统返回的 doc_id 列表
    """
    dcg = 0.0
    for rank, doc_id in enumerate(ranked_doc_ids[:k], start=1):
        rel = rel_dict.get(doc_id, 0)
        if rel > 0:
            dcg += rel / math.log2(rank + 1)

    # 计算理想 DCG（按相关性从大到小排序）
    gains = sorted(rel_dict.values(), reverse=True)
    idcg = 0.0
    for rank, rel in enumerate(gains[:k], start=1):
        if rel > 0:
            idcg += rel / math.log2(rank + 1)

    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def recall_at_k(ranked_doc_ids, rel_dict, k):
    """
    计算单个 query 的 Recall@k。
    """
    if not rel_dict:
        return 0.0
    relevant_set = set(rel_dict.keys())
    retrieved_k = set(ranked_doc_ids[:k])
    hits = len(relevant_set & retrieved_k)
    return hits / len(relevant_set)


def run_manual_evaluation():
    # 1) 加载手动标注数据
    eval_queries = load_manual_qrels(QRELS_PATH)

    # 2) 初始化搜索引擎
    print("🚀 Initializing Search Engine...")
    engine = SearchEngine()

    # 3) 逐个 query 执行检索并记录结果
    all_ndcg = {k: [] for k in K_VALUES}
    all_recall = {k: [] for k in K_VALUES}

    print(f"🔍 Running search for {len(eval_queries)} queries...")
    for query, rel_dict in tqdm(eval_queries, desc="Evaluating"):
        results = engine.search(query, topk=TOPK)
        ranked_doc_ids = [r["doc_id"] for r in results]

        for k in K_VALUES:
            ndcg = ndcg_at_k(ranked_doc_ids, rel_dict, k)
            rec = recall_at_k(ranked_doc_ids, rel_dict, k)
            all_ndcg[k].append(ndcg)
            all_recall[k].append(rec)

    # 4) 汇总并打印整体指标
    print("\n" + "=" * 40)
    print("🏆 MANUAL EVALUATION REPORT")
    print(f"Queries Evaluated: {len(eval_queries)}")
    print("=" * 40)

    for k in K_VALUES:
        mean_ndcg = float(np.mean(all_ndcg[k])) if all_ndcg[k] else 0.0
        mean_recall = float(np.mean(all_recall[k])) if all_recall[k] else 0.0
        print(f"👉 NDCG@{k:<2}: {mean_ndcg:.4f}")
        print(f"👉 Recall@{k:<2}: {mean_recall:.4f}")
        print("-" * 20)

    print("=" * 40)
    print("✅ Done.")


if __name__ == "__main__":
    run_manual_evaluation()
