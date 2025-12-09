import os
import xml.etree.ElementTree as ET
import json
import mwparserfromhell
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XML_FILE = os.path.join(BASE_DIR, "data/raw/simplewiki-latest-pages-articles.xml")
OUT_FILE = os.path.join(BASE_DIR, "data/intermediate/corpus.jsonl")


def normalize_id(title):
    if not title: return ""
    return title.strip().replace(" ", "_")

# Too slow! Using multi-process version instead.
def process_wiki_dump():
    print(f"Parsing XML: {XML_FILE}")
    if not os.path.exists(XML_FILE):
        print("XML file not found!")
        return

    context = ET.iterparse(XML_FILE, events=("end",))
    count = 0
    skipped = 0

    with open(OUT_FILE, "w", encoding="utf-8") as f_out:
        for event, elem in context:
            tag = elem.tag.split("}")[-1]

            if tag == "page":
                title = elem.find("{*}title").text if elem.find("{*}title") is not None else None
                revision = elem.find("{*}revision")
                text_node = revision.find("{*}text") if revision is not None else None
                raw_text = text_node.text if text_node is not None else ""

                ns = elem.find("{*}ns")
                ns_val = int(ns.text) if ns is not None else 0

                if title and raw_text and ns_val == 0 and not raw_text.lower().startswith("#redirect"):

                    try:
                        wikicode = mwparserfromhell.parse(raw_text)

                        clean_text = wikicode.strip_code().strip()


                        links = []
                        for link in wikicode.filter_wikilinks():
                            target = str(link.title)
                            if ":" not in target:
                                links.append(normalize_id(target))

                        # get rid of very short articles
                        if len(clean_text) > 50:
                            doc = {
                                "id": normalize_id(title),
                                "text": clean_text,
                                "out_links": links
                            }
                            f_out.write(json.dumps(doc) + "\n")
                            count += 1
                        else:
                            skipped += 1

                    except Exception as e:
                        print(f"Error parsing {title}: {e}")
                        skipped += 1
                else:
                    skipped += 1

                elem.clear()
                if count % 1000 == 0:
                    print(f"Processed {count} docs...", end='\r', flush=True)

    print(f"\nDone! Saved {count} docs to {OUT_FILE}")


if __name__ == "__main__":
    process_wiki_dump()