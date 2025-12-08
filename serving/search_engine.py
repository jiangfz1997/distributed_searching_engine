import re

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import math
import os
import logging
from contextlib import contextmanager
import sys
from utils import timer
from sentence_transformers import SentenceTransformer, util
import numpy as np

sys.path.append("/app")
from compute.utils.tokenizer import analyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SearchEngine:
    def __init__(self):
        self.pg_host = os.getenv("PG_HOST", "postgres")
        self.pg_user = os.getenv("PG_USER", "admin")
        self.pg_pass = os.getenv("PG_PASS", "password")
        self.pg_db = os.getenv("PG_DB", "search_engine")

        self.k1 = 1.5
        self.b = 0.75
        self.alpha = 0.7
        self.beta = 0.3

        # ====== 语义重排参数（semantic re-ranking）======
        self.enable_semantic = True  # 想临时关掉就改成 False
        self.semantic_topk = 50  # 对前 50 个候选做语义重排
        self.semantic_lambda = 0.6  # lexical 权重 (BM25+PR)
        # (1 - lambda) = 0.3 给 semantic similarity

        if self.enable_semantic:
            print("🔧 Loading semantic model (all-MiniLM-L6-v2)...", flush=True)
            self.semantic_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
            print("✅ Semantic model loaded.", flush=True)
        else:
            self.semantic_model = None

        self._initialize_database_conn_pool()
        self._load_global_stats()
        self._initialize_database_indexes()

    def _initialize_database_conn_pool(self):
        print("🔌 Initializing PostgreSQL Connection Pool...", flush=True)
        import time
        max_retries = 10
        for i in range(max_retries):
            try:
                self.pg_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=20,
                    host=self.pg_host, user=self.pg_user,
                    password=self.pg_pass, dbname=self.pg_db
                )
                print("DB Connection Pool created!", flush=True)
                break
            except Exception as e:
                if i == max_retries - 1: raise e
                print(f"DB not ready yet. Retrying...", flush=True)
                time.sleep(2)

    def _initialize_database_indexes(self):
        """
        Create Indexes on inverted_index, metadata, pagerank
        """
        print("Checking database indexes...", flush=True)
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:

                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_inverted_term 
                        ON inverted_index(term);
                    """)

                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_metadata_doc_id 
                        ON metadata(doc_id);
                    """)

                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_pagerank_doc_id 
                        ON pagerank(doc_id);
                    """)

                conn.commit()

            print("Indexes checked/created successfully.", flush=True)
        except Exception as e:
            print(f"Warning: Failed to create indexes automatically: {e}", flush=True)

    @contextmanager
    def _get_conn(self):
        conn = self.pg_pool.getconn()
        try:
            yield conn
        except Exception as e:
            logger.error(f"DB Error: {e}")
            raise
        finally:
            self.pg_pool.putconn(conn)

    def _load_global_stats(self):
        print("Loading global stats...", flush=True)
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT value FROM config WHERE key=%s", ('avgdl',))
                    row = cur.fetchone()
                    self.avgdl = float(row[0]) if row else 100.0

                    cur.execute("SELECT count(*) FROM metadata")
                    row = cur.fetchone()
                    self.N = int(row[0]) if row else 0
            print(f" Stats loaded: N={self.N}, AvgDL={self.avgdl:.2f}", flush=True)
        except Exception as e:
            print(f"⚠️ Stats failed: {e}", flush=True)
            self.avgdl = 200.0;
            self.N = 100000

    @timer
    def get_metadata_bulk(self, doc_ids):
        res = {}
        if not doc_ids: return res
        clean_map = {did: did.lstrip('_') for did in doc_ids}
        clean_ids = list(set(clean_map.values()))

        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                sql = "SELECT doc_id, length FROM metadata WHERE doc_id IN %s"
                cur.execute(sql, (tuple(clean_ids),))
                temp_data = {row['doc_id']: row['length'] for row in cur.fetchall()}
                for raw_id in doc_ids:
                    clean = clean_map[raw_id]
                    if clean in temp_data: res[raw_id] = temp_data[clean]
        return res
    @timer
    def get_pagerank_bulk(self, doc_ids):
        res = {}
        if not doc_ids: return res
        clean_map = {did: did.lstrip('_') for did in doc_ids}
        clean_ids = list(set(clean_map.values()))

        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                sql = "SELECT doc_id, score FROM pagerank WHERE doc_id IN %s"
                cur.execute(sql, (tuple(clean_ids),))
                temp_scores = {row['doc_id']: row['score'] for row in cur.fetchall()}
                for raw_id in doc_ids:
                    clean = clean_map[raw_id]
                    if clean in temp_scores: res[raw_id] = temp_scores[clean]
        return res

    @timer
    def get_snippets_bulk(self, doc_ids, query_tokens):
        snippets = {}
        if not doc_ids: return snippets
        clean_map = {did: did.lstrip('_') for did in doc_ids}
        clean_ids = list(set(clean_map.values()))

        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                sql = "SELECT doc_id, text FROM metadata WHERE doc_id IN %s"
                cur.execute(sql, (tuple(clean_ids),))
                temp_texts = {row['doc_id']: row['text'] for row in cur.fetchall()}
                for raw_id in doc_ids:
                    clean = clean_map[raw_id]
                    if clean in temp_texts:
                        snippets[raw_id] = self.make_snippet(temp_texts[clean], query_tokens)
        return snippets


    def calculate_bm25(self, tf, doc_length, doc_freq):
        val = (self.N - doc_freq + 0.5) / (doc_freq + 0.5) + 1
        if val <= 0: val = 1.00001
        idf = math.log(val)
        numerator = tf * (self.k1 + 1)
        denominator = tf + self.k1 * (1 - self.b + self.b * (doc_length / self.avgdl))
        return idf * (numerator / denominator)

    @timer
    def _get_inverted_index(self, tokens):
        docs_tracker = {}

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                sql = "SELECT term, df, postings FROM inverted_index WHERE term IN %s"
                cur.execute(sql, (tuple(tokens),))
                rows = cur.fetchall()
                for term, df, postings_dict in rows:
                    if not postings_dict: continue

                    count = 0
                    for doc_id, tf in postings_dict.items():
                        count += 1
                        if count > 20000: break

                        if doc_id not in docs_tracker:
                            docs_tracker[doc_id] = {'matches': []}

                        docs_tracker[doc_id]['matches'].append({
                            'term': term,
                            'tf': tf,
                            'df': df
                        })
        return docs_tracker

    def search(self, query, topk=20, pagerank=True, use_semantics=False, alpha=None, beta=None):
        print(f"🔍 Searching for: {query}, use page rank: {pagerank}, use semantics: {use_semantics}, alpha:{alpha}, beta:{beta}", flush=True)

        if self.N == 0 or self.avgdl == 0.0:
            print(f"Detected self.N == {self.N}, self.avgdl == {self.avgdl}, attempting to reload stats...", flush=True)
            self._load_global_stats()

        if self.N == 0:
            print("Error: Metadata table is empty!", flush=True)
            return []


        # tokenize query
        tokens = analyzer.analyze(query, for_query=True)
        tokens = list(set(tokens))

        if not tokens: return []
        print(f"   Tokens: {tokens}", flush=True)

        # docs_tracker = {}
        #
        # with self._get_conn() as conn:
        #     with conn.cursor() as cur:
        #
        #         sql = "SELECT term, df, postings FROM inverted_index WHERE term IN %s"
        #         cur.execute(sql, (tuple(tokens),))
        #         rows = cur.fetchall()
        #
        #         for term, df, postings_dict in rows:
        #             if not postings_dict: continue
        #
        #             count = 0
        #             for doc_id, tf in postings_dict.items():
        #                 count += 1
        #                 if count > 20000: break
        #
        #                 if doc_id not in docs_tracker:
        #                     docs_tracker[doc_id] = {'matches': []}
        #
        #                 docs_tracker[doc_id]['matches'].append({
        #                     'term': term,
        #                     'tf': tf,
        #                     'df': df
        #                 })
        docs_tracker = self._get_inverted_index(tokens)
        candidate_ids = list(docs_tracker.keys())
        if not candidate_ids: return []

        print(f"   Candidates: {len(candidate_ids)}. Fetching metadata...", flush=True)

        doc_lengths = self.get_metadata_bulk(candidate_ids)
        pr_scores = self.get_pagerank_bulk(candidate_ids)

        scored_results = []
        for doc_id, data in docs_tracker.items():
            doc_len = doc_lengths.get(doc_id, self.avgdl)

            bm25_score = 0.0
            for match in data['matches']:
                bm25_score += self.calculate_bm25(match['tf'], doc_len, match['df'])

            if pagerank:
                pr_score = pr_scores.get(doc_id, 0.0)
                normalized_pr = math.log(1 + pr_score * 100000)
                if alpha is None: alpha = self.alpha
                if beta is None: beta = self.beta
                final_score = (alpha * bm25_score) + (beta * normalized_pr)
            else:
                normalized_pr = 0.0
                final_score = bm25_score

            clean_id = doc_id.replace("_", " ").lower()
            query_lower = query.lower()

            if clean_id == query_lower:
                # 1. 完全匹配奖励 (Exact Match Bonus)
                final_score *= 3.0
            elif query_lower in clean_id:
                # 2. 部分匹配奖励 (Partial Match Bonus)
                final_score *= 1.2
                # ===================================

            scored_results.append({
                "doc_id": doc_id,
                "score": final_score,
                "detail": f"BM25:{bm25_score:.2f} + PR:{normalized_pr:.2f}"
            })

        scored_results.sort(key=lambda x: x['score'], reverse=True)
        if use_semantics and self.semantic_model is not None and scored_results:
            print("   Performing semantic re-ranking...", flush=True)
            scored_results = self.semantic_rerank(query, scored_results, tokens)
        top_results = scored_results[:topk]

        print("   Generating snippets...", flush=True)
        top_ids = [r['doc_id'] for r in top_results]
        snippets_map = self.get_snippets_bulk(top_ids, tokens)

        # 过滤脏数据
        final_list = []
        for res in top_results:
            snippet = snippets_map.get(res['doc_id'], "No content available.")

            # 简单过滤逻辑
            if snippet == "No content available.": continue
            if res['doc_id'].startswith("_born"): continue

            res['snippet'] = snippet
            final_list.append(res)

        return final_list

    def semantic_rerank(self, query, scored_results, tokens):
        # ====== 二阶段语义重排 (semantic re-ranking) ======
        if self.semantic_model is not None and scored_results:
            # 只对前 semantic_topk 个候选做语义打分
            cand_results = scored_results[:self.semantic_topk]
            cand_ids = [r["doc_id"] for r in cand_results]
            print(f"Cand_ids for semantic re-rank: {cand_ids}", flush=True)
            # 先拿一遍 snippet，既用于展示，也可以作为语义模型输入
            print("   [Semantic] Preparing texts for re-ranking...", flush=True)
            # snippets_map = self.get_snippets_bulk(cand_ids, tokens)
            raw_text_map = self.get_raw_text_sample_bulk(cand_ids, limit=300)
            # 构建语义模型输入：这里用 snippet，如果没有就用 doc_id 兜底
            doc_texts = []
            valid_items = []

            for item in cand_results:
                did = item["doc_id"]
                content = raw_text_map.get(did, "")

                # 构造语义输入：Title + Content
                # ID: "Steve_Jobs" -> Title: "Steve Jobs"
                title = did.replace("_", " ")

                # 组合文本 (Transformer 模型通常对开头的文本权重较高)
                semantic_input = f"{title}. {content}"

                doc_texts.append(semantic_input)
                valid_items.append(item)

            if doc_texts:
                # 1) 编码 query（句向量，sentence embedding）
                query_emb = self.semantic_model.encode(
                    query,
                    convert_to_tensor=True,
                    normalize_embeddings=True
                )
                # 2) 编码候选文档文本
                doc_embs = self.semantic_model.encode(
                    doc_texts,
                    convert_to_tensor=True,
                    normalize_embeddings=True
                )
                # 3) 计算余弦相似度 (cosine similarity)
                cos_scores = util.cos_sim(query_emb, doc_embs)[0].cpu().numpy()  # shape: [num_docs]

                # 4) 分数归一化：把 lexical 和 semantic 都映射到 [0,1]
                max_lex = max(item["score"] for item in valid_items) or 1.0
                for item, sem in zip(valid_items, cos_scores):
                    lex_norm = item["score"] / max_lex  # lexical ∈ [0,1]
                    sem_norm = (float(sem) + 1.0) / 2.0  # cosine ∈ [-1,1] → [0,1]
                    combined = (
                            self.semantic_lambda * lex_norm +
                            (1.0 - self.semantic_lambda) * sem_norm
                    )
                    item["combined_score"] = combined

                # 用 combined_score 重排前 semantic_topk 个候选
                valid_items.sort(key=lambda x: x["combined_score"], reverse=True)
                # 之后继续下游流程时，就用 combined score 的顺序 + 原先的 snippet

                # 把重排后的候选放回前面，后面的长尾候选保持原顺序
                scored_results = valid_items + scored_results[self.semantic_topk:]
            else:
                snippets_for_final = None
        else:
            snippets_for_final = None
        # ====== 语义重排结束 ======
        return scored_results
        # 截取最终要返回给用户的 topk

    def get_raw_text_sample_bulk(self, doc_ids, limit=300):
        """批量获取文档原始内容的前 limit 个字符"""
        res = {}
        if not doc_ids: return res

        clean_map = {did: did.lstrip('_') for did in doc_ids}
        clean_ids = list(set(clean_map.values()))

        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 这里的 substring 是数据库层面的截取，节省网络 IO
                sql = "SELECT doc_id, substr(text, 1, %s) as sample FROM metadata WHERE doc_id IN %s"
                cur.execute(sql, (limit, tuple(clean_ids)))

                temp_data = {row['doc_id']: row['sample'] for row in cur.fetchall()}

                for raw_id in doc_ids:
                    clean = clean_map[raw_id]
                    if clean in temp_data:
                        res[raw_id] = temp_data[clean]
        return res

    def make_snippet(self, text, query_tokens, window_size=150):
        """
        生成 Snippet：使用与 analyzer 相同的 spaCy 分词和 lemma，
        在原文中找到第一个命中的 query token，截取一个 window。
        """
        if not text:
            return "No content available."

        # 确保 query_tokens 是 set，查找更快
        qset = set(query_tokens)

        # 使用同一个 spaCy nlp 做分词和 lemma
        # analyzer 是 compute.utils.tokenizer 里的 TextAnalyzer 实例
        doc = analyzer.nlp(text)

        best_span = None

        for token in doc:
            if token.is_space or token.is_punct:
                continue

            lemma = token.lemma_.lower()
            raw_lower = token.text.lower()

            # 只要 lemma 或原词在 query token 中，就认为命中
            if lemma in qset or raw_lower in qset:
                start = token.idx
                end = token.idx + len(token.text)

                snippet_start = max(0, start - window_size // 2)
                snippet_end = min(len(text), end + window_size // 2)

                best_span = (snippet_start, snippet_end)
                break

        if best_span:
            s, e = best_span
            snippet = text[s:e].replace("\n", " ")
            prefix = "..." if s > 0 else ""
            suffix = "..." if e < len(text) else ""
            return f"{prefix}{snippet}{suffix}"
        else:
            # 没找到匹配，返回开头一段
            return text[:window_size].replace("\n", " ") + "..."
