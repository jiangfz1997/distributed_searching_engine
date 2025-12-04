import docker
import redis
import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/admin", tags=["admin"])

# 配置
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
# 我们需要知道 Worker 的镜像名，通常是 项目名_服务名
# 你可以通过 docker ps 确认一下，通常是 distributed-pr-worker 或类似
IMAGE_NAME = "search-compute:v1"
NETWORK_NAME = "distributed_search-net"  # 也是通过 docker network ls 确认

# 连接 Docker 和 Redis
try:
    docker_client = docker.from_env()
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
except Exception as e:
    print(f"⚠️ Admin init failed: {e}")


class ScaleRequest(BaseModel):
    count: int


@router.get("/status")
def get_cluster_status():
    """获取当前集群状态：Worker数量，队列积压情况"""
    try:
        # 1. 统计 Worker 容器数量
        containers = docker_client.containers.list(filters={"ancestor": IMAGE_NAME})
        # 过滤出正在运行 worker.py 的容器
        workers = [c for c in containers if "worker.py" in str(c.attrs['Config']['Cmd'])]
        worker_count = len(workers)

        # 2. 获取 Redis 队列长度
        queue_len = redis_client.llen("queue:pr:tasks")
        processing_len = redis_client.llen("queue:pr:processing")

        # 3. 获取 PageRank 进度
        curr_round = "Unknown"
        # 如果你把 round 写入了 redis，这里可以读。或者读日志文件。

        return {
            "workers": worker_count,
            "queue_pending": queue_len,
            "queue_processing": processing_len,
            "redis_alive": True
        }
    except Exception as e:
        return {"error": str(e), "redis_alive": False}


@router.post("/scale")
def scale_workers(req: ScaleRequest):
    """动态伸缩 Worker"""
    target = req.count
    if target < 0 or target > 20:
        raise HTTPException(400, "Count must be between 0 and 20")

    try:
        # 获取当前 Worker
        containers = docker_client.containers.list(filters={"ancestor": IMAGE_NAME})
        current_workers = [c for c in containers if "worker.py" in str(c.attrs['Config']['Cmd'])]
        curr_count = len(current_workers)

        # 扩容
        if target > curr_count:
            diff = target - curr_count
            for _ in range(diff):
                docker_client.containers.run(
                    image=IMAGE_NAME,
                    command=["python", "compute/pagerank/worker.py"],
                    detach=True,
                    network=NETWORK_NAME,  # 必须加入同一个网络
                    environment=["REDIS_HOST=redis"],
                    volumes={os.getcwd(): {'bind': '/app', 'mode': 'rw'}}  # 挂载代码
                )
            return {"msg": f"Scaled up by {diff}. Total: {target}"}

        # 缩容
        elif target < curr_count:
            diff = curr_count - target
            # 杀掉多余的 (从后面杀)
            for i in range(diff):
                current_workers[i].stop()  # 优雅停止
                current_workers[i].remove()
            return {"msg": f"Scaled down by {diff}. Total: {target}"}

        return {"msg": "No change needed."}

    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/trigger/pagerank")
def trigger_pagerank():
    """一键启动 PageRank Controller"""
    try:
        # 检查是否已经在运行
        existing = docker_client.containers.list(filters={"name": "pr-controller"})
        if existing:
            return {"msg": "Controller is already running!"}

        docker_client.containers.run(
            image=IMAGE_NAME,
            name="pr-controller",  # 固定名字方便查找
            command=["python", "compute/pagerank/controller.py"],
            detach=True,
            network=NETWORK_NAME,
            environment=["REDIS_HOST=redis"],
            volumes={os.getcwd(): {'bind': '/app', 'mode': 'rw'}},
            auto_remove=True  # 跑完自动删除
        )
        return {"msg": "PageRank Controller started!"}
    except Exception as e:
        raise HTTPException(500, str(e))