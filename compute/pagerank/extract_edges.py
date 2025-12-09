import json
import os
from tqdm import tqdm

# Generating edges.tsv from corpus.jsonl for pagerank calculation


DATA_DIR = "/app/data"
INPUT_FILE = os.path.join(DATA_DIR, "intermediate", "corpus.jsonl")
OUTPUT_FILE = os.path.join(DATA_DIR, "edges.tsv")


def extract_edges():
    print(f" Extracting edges from {INPUT_FILE}...")

    if not os.path.exists(INPUT_FILE):
        print(" Error: corpus.jsonl not found!")
        return

    edge_count = 0

    with open(INPUT_FILE, 'r', encoding='utf-8') as f_in, \
            open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:

        for line in tqdm(f_in, desc="Processing"):
            try:
                doc = json.loads(line)
                source_id = doc['id']
                out_links = doc.get('out_links', [])

                for target_id in out_links:
                    # Exclude self edge!! Otherwise PR will explode.
                    if source_id != target_id:
                        f_out.write(f"{source_id}\t{target_id}\n")
                        edge_count += 1

            except json.JSONDecodeError:
                continue

    print(f"Edges extracted! Total edges: {edge_count} Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    extract_edges()