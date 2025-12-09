import json
import os
import sys
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from compute.db_utils import get_db_connection
from compute.utils.tokenizer import analyzer

DATA_DIR = "/app/data"
INPUT_FILE = os.path.join(DATA_DIR, "intermediate", "corpus.jsonl")


def clean_text(text):

    if not text:
        return ""
    return text.replace('\x00', '')


def export_metadata():
    print(f"Connecting to PostgreSQL...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    print(" Truncating 'metadata' table...")
    try:
        cur.execute("TRUNCATE TABLE metadata;")
        conn.commit()
    except Exception as e:
        print(f"Truncate warning: {e}")
        conn.rollback()

    print(f"Extracting metadata from {INPUT_FILE} (using NLTK Analyzer)...")

    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
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
    # tqdm for progress bar
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc="Processing & Tokenizing"):
            try:
                doc = json.loads(line)
                doc_id = doc['id']

                raw_text = doc.get('text', "")
                clean_content = clean_text(raw_text)

                # use the same analyzer as indexing
                tokens = analyzer.analyze(clean_content)
                length = len(tokens)

                total_length += length
                doc_count += 1

                batch_data.append((doc_id, length, clean_content))

            except json.JSONDecodeError:
                continue
            except Exception as e:
                # print(f" Error: {e}")
                continue

            if len(batch_data) >= BATCH_SIZE:
                try:
                    cur.executemany(insert_sql, batch_data)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f" Batch insert failed: {e}")
                batch_data = []

    if batch_data:
        try:
            cur.executemany(insert_sql, batch_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f" Final batch failed: {e}")

    avg_dl = total_length / doc_count if doc_count > 0 else 0.0
    print(f"Statistics: Total Docs={doc_count}, AvgDL={avg_dl:.2f}")

    # Store AvgDL in config table
    try:
        cur.execute("""
            INSERT INTO config (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """, ('avgdl', avg_dl))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"️ Config update failed: {e}")

    cur.close()
    conn.close()
    print(" Metadata export complete!")


if __name__ == "__main__":
    export_metadata()

