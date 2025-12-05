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

    def search(self, query, topk=20):
        print(f"🔍 Searching for: {query}", flush=True)

        if self.N == 0 or self.avgdl == 0.0:
            print(f"Detected self.N == {self.N}, self.avgdl == {self.avgdl}, attempting to reload stats...", flush=True)
            self._load_global_stats()

            # 如果重试后还是 0，那就没办法了，说明库真的是空的
        if self.N == 0:
            print("❌ Error: Metadata table is empty!", flush=True)
            return []


        # tokenize query
        tokens = analyzer.analyze(query)
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

            pr_score = pr_scores.get(doc_id, 0.0)
            normalized_pr = math.log(1 + pr_score * 100000)

            final_score = (self.alpha * bm25_score) + (self.beta * normalized_pr)
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

    def make_snippet(self, text, query_tokens, window_size=150):
        """
        生成 Snippet：基于 Tokenizer 匹配，支持 Stemming，高亮最佳片段。
        """
        if not text: return "No content available."

        # 1. 对原文进行分词，并记录每个词的 (start, end) 位置
        # 使用 NLTK 风格的正则，但我们需要保留位置信息
        # finditer 返回迭代器，包含 match object (span)
        word_iter = re.finditer(r'\b[a-zA-Z]{2,}\b', text)

        best_span = None

        # 2. 遍历原文单词，进行 Stemming 匹配
        for match in word_iter:
            raw_word = match.group()
            # 这里的 stemmed_word 应该和 analyzer.analyze 的逻辑一致
            # 为了性能，我们手动调一下 stemmer，不再调 analyzer (因为 analyzer 会去停用词)
            # 我们希望即使是停用词也能保留位置
            from nltk.stem import SnowballStemmer
            stemmer = SnowballStemmer("english")
            stemmed_word = stemmer.stem(raw_word.lower())

            if stemmed_word in query_tokens:
                # 找到匹配！
                start, end = match.span()

                # 3. 确定窗口范围
                # 尝试以该词为中心
                snippet_start = max(0, start - window_size // 2)
                snippet_end = min(len(text), end + window_size // 2)

                # 稍微调整边界，避免截断单词 (可选优化，这里简单截断)
                best_span = (snippet_start, snippet_end)
                break  # 找到第一个匹配就返回 (简单策略)
                # 进阶策略：继续找，看哪个窗口包含的 query_tokens 最多 (Dense Window)

        # 4. 生成结果
        if best_span:
            s, e = best_span
            snippet = text[s:e].replace('\n', ' ')
            # 如果不是从头开始，加省略号
            prefix = "..." if s > 0 else ""
            suffix = "..." if e < len(text) else ""
            return f"{prefix}{snippet}{suffix}"
        else:
            # 没找到匹配词 (可能是停用词匹配，或者 query tokens 被过滤没了)
            # 返回开头一段
            return text[:window_size] + "..."