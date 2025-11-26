import os
import xml.etree.ElementTree as ET
import json
import re
from tqdm import tqdm

# ================= 路径配置 =================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
XML_FILENAME = "simplewiki-latest-pages-articles.xml"
RAW_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", XML_FILENAME)
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "intermediate", "corpus.jsonl")


# ===========================================

def normalize_id(title):
    if not title: return ""
    return title.strip().replace(" ", "_")


def clean_and_extract_links(wiki_text):
    if not wiki_text: return "", []
    out_links = []
    # 匹配 [[Target|Label]] 或 [[Target]]
    pattern = re.compile(r'\[\[([^|\]]+)(?:\|([^\]]+))?\]\]')

    def replace_func(match):
        target = match.group(1)
        label = match.group(2) if match.group(2) else target
        if ":" not in target:
            out_links.append(normalize_id(target))
        return label

    text_step1 = pattern.sub(replace_func, wiki_text)
    return text_step1, out_links


def strip_tag_name(t):
    """
    辅助函数：去掉 {http://...} 这种前缀
    """
    if '}' in t:
        return t.split('}', 1)[1]
    return t


def process_xml():
    print(f"📂 读取文件: {RAW_FILE_PATH}")
    if not os.path.exists(RAW_FILE_PATH):
        print("❌ 文件不存在！请检查路径。")
        return

    print(f"🚀 开始处理 XML...")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f_out:
        # 使用 iterparse 流式读取
        context = ET.iterparse(RAW_FILE_PATH, events=("end",))

        for event, elem in context:
            # 获取去除了命名空间的标签名
            tag = strip_tag_name(elem.tag)

            if tag == "page":
                title = None
                text = None

                # 遍历子节点查找 title 和 text
                for child in elem:
                    child_tag = strip_tag_name(child.tag)
                    if child_tag == "title":
                        title = child.text
                    elif child_tag == "revision":
                        for rev_child in child:
                            if strip_tag_name(rev_child.tag) == "text":
                                text = rev_child.text
                                break

                if title and text:
                    # 过滤特殊页面
                    if ":" not in title:
                        doc_id = normalize_id(title)
                        clean_text, links = clean_and_extract_links(text)

                        doc = {
                            "id": doc_id,
                            "text": clean_text,
                            "out_links": links
                        }
                        f_out.write(json.dumps(doc) + "\n")
                        count += 1

                        if count % 1000 == 0:
                            print(f"✅ 已生成 {count} 条数据...", end="\r")

                # --- 修复点：标准库的内存清理方式 ---
                # 只需 clear 即可，不要用 getprevious
                elem.clear()

            # 这里不需要 else 打印了，避免刷屏

    print(f"\n✨ 处理完成！共生成 {count} 条数据。")
    print(f"📁 结果保存在: {OUTPUT_FILE}")


if __name__ == "__main__":
    process_xml()