import os
import xml.etree.ElementTree as ET
import json
import mwparserfromhell
import multiprocessing
from functools import partial
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XML_FILE = os.path.join(BASE_DIR, "data/raw/simplewiki-latest-pages-articles.xml")
OUT_FILE = os.path.join(BASE_DIR, "data/intermediate/corpus.jsonl")

NUM_WORKERS = max(1, multiprocessing.cpu_count() - 1)


def normalize_id(title):
    if not title: return ""
    return title.strip().replace(" ", "_")


def parse_worker(task_data):

    title, raw_text = task_data

    try:
        wikicode = mwparserfromhell.parse(raw_text)

        clean_text = wikicode.strip_code().strip()

        links = []
        for link in wikicode.filter_wikilinks():
            target = str(link.title)
            if ":" not in target:
                links.append(normalize_id(target))

        if len(clean_text) > 50:
            return json.dumps({
                "id": normalize_id(title),
                "text": clean_text,
                "out_links": links
            })
    except Exception:
        return None
    return None


def process_wiki_dump_parallel():
    print(f"Parsing XML: {XML_FILE}")
    print(f"Starting Multiprocessing Pool with {NUM_WORKERS} workers...")

    if not os.path.exists(XML_FILE):
        print("XML file not found!")
        return


    f_out = open(OUT_FILE, "w", encoding="utf-8")


    pool = multiprocessing.Pool(processes=NUM_WORKERS)

    context = ET.iterparse(XML_FILE, events=("end",))


    batch_size = 1000
    batch_data = []

    count = 0


    start_time = time.time()

    for event, elem in context:
        tag = elem.tag.split("}")[-1]

        if tag == "page":
            title = elem.find("{*}title").text
            revision = elem.find("{*}revision")
            text_node = revision.find("{*}text")
            raw_text = text_node.text if text_node is not None else ""
            ns = elem.find("{*}ns")
            ns_val = int(ns.text) if ns is not None else 0


            if title and raw_text and ns_val == 0 and not raw_text.lower().startswith("#redirect"):
                batch_data.append((title, raw_text))

             # Try using multiprocessing to process the batch
            if len(batch_data) >= batch_size:

                results = pool.imap_unordered(parse_worker, batch_data)

                for res in results:
                    if res:
                        f_out.write(res + "\n")
                        count += 1
                        if count % 1000 == 0:
                            elapsed = time.time() - start_time
                            speed = count / elapsed
                            print(f"Processed {count} docs... (Speed: {speed:.2f} docs/s)", flush=True)

                batch_data = []

            elem.clear()

    if batch_data:
        results = pool.imap_unordered(parse_worker, batch_data)
        for res in results:
            if res:
                f_out.write(res + "\n")
                count += 1

    pool.close()
    pool.join()
    f_out.close()

    print(f"\nDone! Saved {count} docs to {OUT_FILE}")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    process_wiki_dump_parallel()