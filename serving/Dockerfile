FROM python:3.9-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*
# 1. 安装依赖
# 注意路径变了：现在是相对于根目录
COPY serving/requirements.txt .
# 还需要安装 compute 模块需要的依赖 (比如 nltk, mwparserfromhell)
# 简单起见，我们直接在这里把 compute 需要的包也装上
RUN pip install --no-cache-dir -r requirements.txt

# 2. 精准复制需要的代码文件夹 (不要 COPY . .)
# 把 compute 文件夹拷进去，这样 backend 就能 import compute.tokenizer 了
COPY compute/ /app/compute/
COPY serving/ /app/serving/

# 3. 设置环境变量
# 这样 Python 就能在 /app 下找到 compute 和 serving 包
ENV PYTHONPATH=/app

# 4. 暴露端口
EXPOSE 8000

# 5. 启动命令
# 注意：因为 main.py 在 serving 目录下，我们需要以模块方式运行
# 或者路径写成 serving.main:app
CMD ["python", "-m", "uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]