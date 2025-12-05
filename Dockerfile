FROM python:3.9-slim

WORKDIR /app

# 安装依赖：tqdm用于显示进度，redis用于连接Redis

# 将根目录下的代码全部复制进容器
# 这样容器里就有了 /app/compute/indexing/...
#COPY . /app/

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# 设置 Python 路径，防止 ModuleNotFoundError
ENV PYTHONPATH=/app

CMD ["bash"]