import os
import xml.etree.ElementTree as ET
import json
import mwparserfromhell  # <--- 神器
from tqdm import tqdm

# === 配置 ===
# 确保这里路径对得上
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XML_FILE = os.path.join(BASE_DIR, "data/raw/simplewiki-latest-pages-articles.xml")
OUT_FILE = os.path.join(BASE_DIR, "data/intermediate/corpus.jsonl")


def normalize_id(title):
    if not title: return ""
    return title.strip().replace(" ", "_")


def process_wiki_dump():
    print(f"🚀 Parsing XML: {XML_FILE}")
    if not os.path.exists(XML_FILE):
        print("❌ XML file not found!")
        return

    context = ET.iterparse(XML_FILE, events=("end",))
    count = 0
    skipped = 0

    with open(OUT_FILE, "w", encoding="utf-8") as f_out:
        for event, elem in context:
            # 处理 XML 命名空间: {http://...}page -> page
            tag = elem.tag.split("}")[-1]

            if tag == "page":
                title = elem.find("{*}title").text if elem.find("{*}title") is not None else None
                revision = elem.find("{*}revision")
                text_node = revision.find("{*}text") if revision is not None else None
                raw_text = text_node.text if text_node is not None else ""

                # 过滤重定向和非主命名空间
                ns = elem.find("{*}ns")
                ns_val = int(ns.text) if ns is not None else 0

                # 必须是主条目(ns=0)，且不是 Redirect
                if title and raw_text and ns_val == 0 and not raw_text.lower().startswith("#redirect"):

                    try:
                        # === 核心魔法：使用 mwparserfromhell 解析 ===
                        wikicode = mwparserfromhell.parse(raw_text)

                        # 1. 提取纯文本 (自动去掉 {{...}}, <ref>, '''...''')
                        clean_text = wikicode.strip_code().strip()

                        # 2. 提取出链 (PageRank 需要!)
                        # filter_wikilinks() 会自动找到 [[Target]]
                        links = []
                        for link in wikicode.filter_wikilinks():
                            # 获取链接目标 (e.g. "United States")
                            target = str(link.title)
                            # 过滤掉文件和分类链接
                            if ":" not in target:
                                links.append(normalize_id(target))

                        # 写入结果
                        if len(clean_text) > 50:  # 太短的丢掉
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
                        print(f"⚠️ Error parsing {title}: {e}")
                        skipped += 1
                else:
                    skipped += 1

                # 清理内存
                elem.clear()
                if count % 1000 == 0:
                    print(f"✅ Processed {count} docs...", end='\r')

    print(f"\n✨ Done! Saved {count} docs to {OUT_FILE}")


if __name__ == "__main__":
    process_wiki_dump()