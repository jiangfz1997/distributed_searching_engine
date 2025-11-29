FROM python:3.9-slim

WORKDIR /app

# 1. 安装评估专用依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. 预下载 NLTK 数据 (可选，防止每次运行脚本都下载)
RUN python -m nltk.downloader punkt stopwords

# 3. 设置 PYTHONPATH，确保能 import 根目录下的 compute 和 serving
ENV PYTHONPATH=/app

# 4. 默认命令：保持容器存活
CMD ["tail", "-f", "/dev/null"]