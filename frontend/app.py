import streamlit as st
import requests
import time

# === 配置 ===
# Docker 内部通信地址：使用服务名 "backend"
# 如果是在本地运行不走Docker，则用 localhost
BACKEND_URL = "http://backend:8000"

# === 页面设置 ===
st.set_page_config(
    page_title="SimpleWiki Search",
    page_icon="🔎",
    layout="centered"
)

# === 自定义 CSS (让界面更好看) ===
st.markdown("""
<style>
    .result-card {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        border-left: 5px solid #4F8BF9;
    }
    .result-title {
        font-size: 1.2em;
        font-weight: bold;
        color: #1a0dab;
        text-decoration: none;
    }
    .result-snippet {
        font-size: 0.95em;
        color: #4d5156;
        margin-top: 5px;
    }
    .result-meta {
        font-size: 0.8em;
        color: #006621;
        margin-top: 8px;
    }
    .stTextInput>div>div>input {
        border-radius: 20px;
    }
</style>
""", unsafe_allow_html=True)

# === 侧边栏 ===
with st.sidebar:
    st.header("⚙️ Search Settings")
    top_k = st.slider("Max Results", 5, 50, 10)
    st.info("Backend: FastAPI + PostgreSQL\nAlgorithm: BM25 + PageRank")

# === 主界面 ===
st.title("🔎 Wiki Search Engine")

# 搜索框 (回车触发)
query = st.text_input("", placeholder="Search for something (e.g. 'United States')...")

# === 搜索逻辑 ===
if query:
    start_time = time.time()
    try:
        # 发送请求给 Backend
        response = requests.get(
            f"{BACKEND_URL}/search",
            params={"q": query, "limit": top_k},
            timeout=5
        )

        if response.status_code == 200:
            results = response.json()
            duration = time.time() - start_time

            # 显示统计信息
            st.caption(f"Found {len(results)} results in {duration:.4f} seconds.")

            if not results:
                st.warning("No results found. Try a different keyword.")

            # 渲染结果列表
            for res in results:
                # 处理标题：把下划线换成空格
                display_title = res['doc_id'].replace("_", " ")
                # 生成维基百科链接
                wiki_link = f"https://simple.wikipedia.org/wiki/{res['doc_id']}"

                # 使用 HTML 卡片展示
                st.markdown(f"""
                <div class="result-card">
                    <a href="{wiki_link}" target="_blank" class="result-title">{display_title}</a>
                    <div class="result-snippet">{res['snippet']}</div>
                    <div class="result-meta">Score: {res['score']:.4f} | {res['detail']}</div>
                </div>
                """, unsafe_allow_html=True)

        else:
            st.error(f"Error {response.status_code}: {response.text}")

    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to Backend. Is the Docker container running?")
    except Exception as e:
        st.error(f"An error occurred: {e}")