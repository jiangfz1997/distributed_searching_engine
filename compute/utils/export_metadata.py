import json
import os
import sys
from tqdm import tqdm

# 1. 路径设置：确保能引用到 compute 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from compute.db_utils import get_db_connection
# 2. 引入共享分词器 (NLTK Analyzer)
from compute.utils.tokenizer import analyzer

# === 配置 ===
DATA_DIR = "/app/data"
INPUT_FILE = os.path.join(DATA_DIR, "intermediate", "corpus.jsonl")


def clean_text(text):
    """
    清洗 Postgres 不支持的字符 (NUL Byte)
    """
    if not text:
        return ""
    return text.replace('\x00', '')


def export_metadata():
    print(f"🔌 Connecting to PostgreSQL...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    # 1. 清理旧数据
    print("🧹 Truncating 'metadata' table...")
    try:
        cur.execute("TRUNCATE TABLE metadata;")
        conn.commit()
    except Exception as e:
        print(f"⚠️ Truncate warning: {e}")
        conn.rollback()

    print(f"🚀 Extracting metadata from {INPUT_FILE} (using NLTK Analyzer)...")

    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    batch_data = []
    BATCH_SIZE = 2000

    total_length = 0
    doc_count = 0

    insert_sql = """
        INSERT INTO metadata (doc_id, length, text) 
        VALUES (%s, %s, %s)
        ON CONFLICT (doc_id) DO UPDATE 
        SET length = EXCLUDED.length, text = EXCLUDED.text;
    """

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        # 使用 tqdm 显示进度
        for line in tqdm(f, desc="Processing & Tokenizing"):
            try:
                doc = json.loads(line)
                doc_id = doc['id']

                # 清洗文本 (用于存储和展示 Snippet)
                raw_text = doc.get('text', "")
                clean_content = clean_text(raw_text)

                # === 核心修改：使用 NLTK Analyzer 计算“有效长度” ===
                # 这里的 length 不再是单词数，而是“去停用词后、词干提取后的有效词根数”
                # 这与 Indexing 阶段完全对齐，保证 BM25 计算的科学性
                tokens = analyzer.analyze(clean_content)
                length = len(tokens)

                # 收集统计信息
                total_length += length
                doc_count += 1

                batch_data.append((doc_id, length, clean_content))

            except json.JSONDecodeError:
                continue
            except Exception as e:
                # print(f"⚠️ Error: {e}")
                continue

            # 批量写入
            if len(batch_data) >= BATCH_SIZE:
                try:
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"⚠️ Batch insert failed: {e}")
                batch_data = []

    # 写入剩余
    if batch_data:
        try:
            cur.executemany(insert_sql, batch_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"⚠️ Final batch failed: {e}")

    # 2. 计算并存储全局统计量 (AvgDL)
    avg_dl = total_length / doc_count if doc_count > 0 else 0.0
    print(f"📊 Statistics: Total Docs={doc_count}, AvgDL={avg_dl:.2f}")

    # 存入 config 表
    try:
        cur.execute("""
            INSERT INTO config (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """, ('avgdl', avg_dl))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Config update failed: {e}")

    cur.close()
    conn.close()
    print("✅ Metadata export complete (NLTK Consistent)!")


if __name__ == "__main__":
    export_metadata()