import os
import xml.etree.ElementTree as ET
import json
import mwparserfromhell
import multiprocessing
from functools import partial
import time

# === 配置 ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XML_FILE = os.path.join(BASE_DIR, "data/raw/simplewiki-latest-pages-articles.xml")
OUT_FILE = os.path.join(BASE_DIR, "data/intermediate/corpus.jsonl")

# 设定并行数量 (默认使用所有 CPU 核心)
NUM_WORKERS = max(1, multiprocessing.cpu_count() - 1)


def normalize_id(title):
    if not title: return ""
    return title.strip().replace(" ", "_")


def parse_worker(task_data):
    """
    这是运行在子进程里的函数。
    task_data: (title, raw_text)
    """
    title, raw_text = task_data

    try:
        # === 耗时操作在这里并行执行 ===
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
    print(f"🚀 Parsing XML: {XML_FILE}")
    print(f"🔥 Starting Multiprocessing Pool with {NUM_WORKERS} workers...")

    if not os.path.exists(XML_FILE):
        print("❌ XML file not found!")
        return

    # 准备写入文件
    f_out = open(OUT_FILE, "w", encoding="utf-8")

    # 启动进程池
    pool = multiprocessing.Pool(processes=NUM_WORKERS)

    context = ET.iterparse(XML_FILE, events=("end",))

    # 批处理队列
    batch_size = 1000
    batch_data = []

    count = 0

    # 计时开始
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

            # 只有主条目才放入队列
            if title and raw_text and ns_val == 0 and not raw_text.lower().startswith("#redirect"):
                batch_data.append((title, raw_text))

            # 攒够一波数据，或者 XML 读完了，就发给工人去干活
            if len(batch_data) >= batch_size:
                # 提交任务到进程池
                # imap_unordered 比 map 更快，因为不保证顺序（我们也不需要顺序）
                results = pool.imap_unordered(parse_worker, batch_data)

                # 收集结果并写入
                for res in results:
                    if res:
                        f_out.write(res + "\n")
                        count += 1
                        if count % 1000 == 0:
                            elapsed = time.time() - start_time
                            speed = count / elapsed
                            print(f"✅ Processed {count} docs... (Speed: {speed:.2f} docs/s)", flush=True)

                # 清空批次
                batch_data = []

            elem.clear()

    # 处理最后一波剩余的数据
    if batch_data:
        results = pool.imap_unordered(parse_worker, batch_data)
        for res in results:
            if res:
                f_out.write(res + "\n")
                count += 1

    pool.close()
    pool.join()
    f_out.close()

    print(f"\n✨ Done! Saved {count} docs to {OUT_FILE}")


if __name__ == "__main__":
    # Windows/Mac 上 multiprocessing 需要这个保护
    multiprocessing.freeze_support()
    process_wiki_dump_parallel()