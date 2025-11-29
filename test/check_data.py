import json

INPUT_FILE = "../data/intermediate/corpus.jsonl"


def verify_jsonl():
    print(f"🕵️‍♀️ 正在验尸: {INPUT_FILE} ...")
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 5: break  # 只看前 5 行

                # 尝试解析
                data = json.loads(line)

                # 检查字段
                print(f"\n--- Line {i + 1} ---")
                print(f"ID:   {data.get('id')}")
                print(f"Text: {data.get('text')[:50]}...")  # 只打印前50个字符
                print(f"Links: {data.get('out_links')[:3]}...")  # 只打印前3个链接

                # 验证字段是否存在
                if "id" not in data or "text" not in data:
                    print("❌ 缺少关键字段！")
                    return

        print("\n✅ 文件格式验证通过！是标准的 JSONL。")

    except FileNotFoundError:
        print("❌ 文件都没生成，肯定卡在之前的步骤了。")
    except json.JSONDecodeError as e:
        print(f"❌ JSON 解析失败 (Line {i + 1}): {e}")


if __name__ == "__main__":
    verify_jsonl()