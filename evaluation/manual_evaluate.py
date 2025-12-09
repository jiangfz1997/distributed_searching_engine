# Manual evaluation
import os
import sys
import json
import math
import numpy as np
from tqdm import tqdm

sys.path.append("/app")
from serving.search_engine import SearchEngine

QRELS_PATH = "/app/evaluation/eval_data/eval_semantic.json"
TOPK = 20
K_VALUES = [1, 3, 5, 10]


def load_manual_qrels(path: str):
    if not os.path.exists(path):
        print(f"Qrels file not found: {path}")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    queries = []
    for item in data:
        query = item["query"]
        rel = item["relevant"]

        if isinstance(rel, list):
            rel_dict = {doc_id: 1 for doc_id in rel}
        else:
            rel_dict = dict(rel)

        queries.append((query, rel_dict))

    print(f"Loaded {len(queries)} manually labeled queries.")
    return queries


def ndcg_at_k(ranked_doc_ids, rel_dict, k):
    dcg = 0.0
    for rank, doc_id in enumerate(ranked_doc_ids[:k], start=1):
        rel = rel_dict.get(doc_id, 0)
        if rel > 0:
            dcg += rel / math.log2(rank + 1)

    gains = sorted(rel_dict.values(), reverse=True)
    idcg = 0.0
    for rank, rel in enumerate(gains[:k], start=1):
        if rel > 0:
            idcg += rel / math.log2(rank + 1)

    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def recall_at_k(ranked_doc_ids, rel_dict, k):
    if not rel_dict:
        return 0.0
    relevant_set = set(rel_dict.keys())
    retrieved_k = set(ranked_doc_ids[:k])
    hits = len(relevant_set & retrieved_k)
    return hits / len(relevant_set)


def run_manual_evaluation(path: str = QRELS_PATH):
    print("Loading manual relevance judgments...")
    eval_queries = load_manual_qrels(path)

    print("Initializing Search Engine...")
    engine = SearchEngine()

    all_ndcg = {k: [] for k in K_VALUES}
    all_recall = {k: [] for k in K_VALUES}

    print(f"Running search for {len(eval_queries)} queries...\n")


    for idx, (query, rel_dict) in enumerate(eval_queries):
        results = engine.search(query, topk=TOPK, alpha=0.5, beta=0.5)


        ranked_doc_ids = [r["doc_id"].lstrip('_') for r in results]

        print(f"[{idx + 1}/{len(eval_queries)}] Query: '{query}'")


        hits = [doc for doc in ranked_doc_ids[:10] if doc in rel_dict]

        print(f"   Target (Top 3): {list(rel_dict.keys())[:3]}...")
        print(f"   Result (Top 5): {ranked_doc_ids[:5]}")

        if hits:
            print(f"   Hits in Top 10: {hits}")
        else:
            print(f"   No hits in Top 10.")
        print("-" * 50)


        for k in K_VALUES:
            ndcg = ndcg_at_k(ranked_doc_ids, rel_dict, k)
            rec = recall_at_k(ranked_doc_ids, rel_dict, k)
            all_ndcg[k].append(ndcg)
            all_recall[k].append(rec)

    print("\n" + "=" * 40)
    print("MANUAL EVALUATION REPORT")
    print(f"Queries Evaluated: {len(eval_queries)}")
    print("=" * 40)

    for k in K_VALUES:
        mean_ndcg = float(np.mean(all_ndcg[k])) if all_ndcg[k] else 0.0
        mean_recall = float(np.mean(all_recall[k])) if all_recall[k] else 0.0
        print(f" -> NDCG@{k:<2}: {mean_ndcg:.4f}")
        print(f" -> Recall@{k:<2}: {mean_recall:.4f}")
        print("-" * 20)

    print("=" * 40)
    print("Done.")


if __name__ == "__main__":
    run_manual_evaluation()