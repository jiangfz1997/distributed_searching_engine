import os
import psycopg2
from psycopg2.extras import RealDictCursor

# 从环境变量读取配置
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "password")
PG_DB = os.getenv("PG_DB", "search_engine")


def get_db_connection():
    """获取 Postgres 连接"""
    conn = psycopg2.connect(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB
    )
    return conn


def init_tables():
    """初始化所有表结构"""
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. 倒排索引表
    cur.execute("""
            CREATE TABLE IF NOT EXISTS inverted_index (
                term TEXT PRIMARY KEY,
                df INTEGER, 
                postings JSONB
            );
        """)

    # 2. PageRank 表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pagerank (
            doc_id TEXT PRIMARY KEY,
            score DOUBLE PRECISION
        );
    """)

    # 3. Metadata 表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            doc_id TEXT PRIMARY KEY,
            length INTEGER,
            text TEXT
        );
    """)

    # 4. Config 表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value DOUBLE PRECISION
        );
    """)

    conn.commit()
    conn.close()
    print("✅ PostgreSQL tables initialized.")


if __name__ == "__main__":
    init_tables()