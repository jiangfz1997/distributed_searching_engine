import streamlit as st
import requests
import pandas as pd
import time
import plotly.express as px  # 需要在 requirements.txt 加 plotly

API_URL = "http://backend:8000"

st.set_page_config(page_title="Cluster Dashboard", page_icon="🎛️", layout="wide")

st.title("🎛️ Distributed Cluster Dashboard")

# 侧边栏控制区
with st.sidebar:
    st.header("🎮 Actions")

    # 一键启动按钮
    if st.button("🚀 Start PageRank Job"):
        try:
            res = requests.post(f"{API_URL}/admin/trigger/pagerank")
            if res.status_code == 200:
                st.success("Job Started!")
            else:
                st.error(res.text)
        except:
            st.error("Failed to trigger job")

    st.divider()

    # 扩缩容控制
    st.subheader("⚖️ Scaling")
    target_workers = st.number_input("Target Worker Count", min_value=0, max_value=20, value=4)
    if st.button("Apply Scale"):
        try:
            res = requests.post(f"{API_URL}/admin/scale", json={"count": target_workers})
            st.info(res.json().get("msg"))
        except Exception as e:
            st.error(f"Scaling failed: {e}")

# 实时监控区 (使用 empty 容器实现刷新)
placeholder = st.empty()

# 自动刷新循环
while True:
    try:
        # 获取状态
        resp = requests.get(f"{API_URL}/admin/status", timeout=2)
        if resp.status_code == 200:
            data = resp.json()

            with placeholder.container():
                # 第一排：关键指标
                k1, k2, k3 = st.columns(3)
                k1.metric("👷 Active Workers", data.get("workers", 0))
                k2.metric("📥 Pending Tasks", data.get("queue_pending", 0))
                k3.metric("⚙️ Processing Tasks", data.get("queue_processing", 0))

                # 第二排：图表 (模拟历史数据，或者你可以在后端存历史)
                # 这里简单展示 Pending vs Processing
                chart_data = pd.DataFrame({
                    "Type": ["Pending", "Processing"],
                    "Count": [data.get("queue_pending", 0), data.get("queue_processing", 0)]
                })

                fig = px.bar(chart_data, x="Type", y="Count", title="Queue Status", color="Type")
                st.plotly_chart(fig, use_container_width=True)

                # 状态指示灯
                if data.get("redis_alive"):
                    st.success("🟢 Redis Connection: Healthy")
                else:
                    st.error("🔴 Redis Connection: Failed")

        else:
            placeholder.error("Failed to fetch status from backend.")

    except Exception as e:
        placeholder.error(f"Connection Error: {e}")

    # 每 2 秒刷新一次
    time.sleep(2)