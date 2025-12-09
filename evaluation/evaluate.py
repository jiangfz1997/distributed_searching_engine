# Evaluate the search engine on the DBpedia-Entity BEIR benchmark
# Simple wiki data is not matching BeIR IDs and have lots of missing docs
# This script tries to find intersections and evaluate only on those
# Maybe there's a better evaluation dataset or use full wiki dump later

import sys
import os
import math
import numpy as np
from tqdm import tqdm
from beir import util
from beir.datasets.data_loader import GenericDataLoader
from urllib.parse import unquote
import json
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("/app")

from serving.search_engine import SearchEngine
from compute.db_utils import get_db_connection

DATA_DIR = "/app/data"
BENCHMARK_NAME = "dbpedia-entity"
DATA_PATH = os.path.join(DATA_DIR, "benchmark", BENCHMARK_NAME)


def load_local_doc_ids():
    print(" Loading local DocIDs from PostgreSQL...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT doc_id FROM metadata")
        local_ids = set(row[0] for row in cur.fetchall())
        conn.close()


        print(f"Loaded {len(local_ids)} local docs.")
        print(f"[DEBUG] Local ID Samples (Postgres): {list(local_ids)[:5]}")
        return local_ids
    except Exception as e:
        print(f"DB Error: {e}")
        return set()


def try_match_id(beir_raw_id, local_ids_set):
    # Try to find a matching local ID for the given BeIR raw ID
    candidates = []


    clean_id = beir_raw_id.replace("<dbpedia:", "").replace(">", "")
    candidates.append(clean_id)

    decoded = unquote(clean_id)
    candidates.append(decoded)


    underscore_id = decoded.replace(" ", "_")
    candidates.append(underscore_id)


    if underscore_id:
        cap_id = underscore_id[0].upper() + underscore_id[1:]
        candidates.append(cap_id)

    for cand in candidates:
        if cand in local_ids_set:
            return cand

    return None


def calculate_metrics(run_results, qrels, k_values=[1, 10, 100]):
    metrics = {k: {"ndcg": [], "recall": []} for k in k_values}

    for qid, ranked_list in run_results.items():
        if qid not in qrels: continue
        target_map = qrels[qid]

        for k in k_values:
            cut_list = ranked_list[:k]
            hits = sum(1 for doc in cut_list if doc in target_map)
            total_targets = len(target_map)
            recall = hits / total_targets if total_targets > 0 else 0.0
            metrics[k]["recall"].append(recall)

            dcg = 0.0
            idcg = 0.0
            for i, doc in enumerate(cut_list):
                if doc in target_map:
                    rel = target_map[doc]
                    dcg += rel / math.log2((i + 1) + 1)

            ideal_rels = sorted(target_map.values(), reverse=True)
            for i, rel in enumerate(ideal_rels[:k]):
                idcg += rel / math.log2((i + 1) + 1)

            ndcg = (dcg / idcg) if idcg > 0 else 0.0
            metrics[k]["ndcg"].append(ndcg)

    return metrics


import csv


def load_beir_lightweight(data_path):

    print("⚡ Using lightweight loader...")

    queries = {}
    query_file = os.path.join(data_path, "queries.jsonl")
    with open(query_file, 'r', encoding='utf-8') as f:
        for line in f:
            item = json.loads(line)
            queries[item['_id']] = item['text']

    qrels = {}
    qrels_file = os.path.join(data_path, "qrels", "test.tsv")
    with open(qrels_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        next(reader)
        for row in reader:
            qid, doc_id, score = row[0], row[1], int(row[2])
            if qid not in qrels:
                qrels[qid] = {}
            qrels[qid][doc_id] = score

    print(f"Loaded {len(queries)} queries and {len(qrels)} qrels sets.")
    return queries, qrels
def run_evaluation():
    if not os.path.exists(DATA_PATH):
        url = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip".format(BENCHMARK_NAME)
        util.download_and_unzip(url, os.path.join(DATA_DIR, "benchmark"))

    print("Loading Benchmark Qrels...")
    # _, queries, qrels = GenericDataLoader(DATA_PATH).load(split="test")
    queries, qrels = load_beir_lightweight(DATA_PATH)
    local_ids = load_local_doc_ids()
    if not local_ids: return

    print("Filtering queries (SimpleWiki ∩ BeIR)...")

    valid_qrels = {}
    valid_queries = {}

    debug_miss_count = 0

    for qid, target_map in qrels.items():
        new_target_map = {}
        for beir_doc_id, rel in target_map.items():
            # print(f"Try to match beir_doc_id: {beir_doc_id}")
            matched_id = try_match_id(beir_doc_id, local_ids)

            if matched_id:
                new_target_map[matched_id] = rel
            else:
                if debug_miss_count < 5:
                    print(f"[MISS] BeIR ID: '{beir_doc_id}' vs Local Samples...")
                    debug_miss_count += 1

        if new_target_map:
            valid_qrels[qid] = new_target_map
            valid_queries[qid] = queries[qid]

    print(f"Retention: {len(valid_queries)} / {len(queries)} queries.")

    if len(valid_queries) == 0:
        print("Still no intersection! Check the [MISS] logs above.")
        return

    print("Initializing Search Engine...")
    engine = SearchEngine()

    print(f"Running search on {len(valid_queries)} queries...")
    run_results = {}

    for qid, query_text in tqdm(valid_queries.items(), desc="Searching"):
        search_res = engine.search(query_text, topk=100)
        run_results[qid] = [r['doc_id'] for r in search_res]

    print("\nCalculating Metrics...")
    final_metrics = calculate_metrics(run_results, valid_qrels)

    print("\n" + "=" * 40)
    print(f"EVALUATION REPORT")
    print(f"Queries Evaluated: {len(valid_queries)}")
    print("=" * 40)
    for k in [1, 10, 100]:
        ndcg = np.mean(final_metrics[k]["ndcg"])
        recall = np.mean(final_metrics[k]["recall"])
        print(f" -> NDCG@{k:<3} : {ndcg:.4f}")
        print(f" -> Recall@{k:<3}: {recall:.4f}")
        print("-" * 20)
    print("=" * 40)


if __name__ == "__main__":
    run_evaluation()