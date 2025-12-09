import docker
import redis
import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/admin", tags=["admin"])

REDIS_HOST = os.getenv("REDIS_HOST", "redis")

# TODO: use config file or env vars
IMAGE_NAME = "search-compute:lite"
NETWORK_NAME = "distributed_search-net"

try:
    docker_client = docker.from_env()
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
except Exception as e:
    print(f"⚠️ Admin init failed: {e}")


class ScaleRequest(BaseModel):
    count: int

# Not using for now
@router.get("/status")
def get_cluster_status():
    try:
        containers = docker_client.containers.list(filters={"ancestor": IMAGE_NAME})
        workers = [c for c in containers if "worker.py" in str(c.attrs['Config']['Cmd'])]
        worker_count = len(workers)

        queue_len = redis_client.llen("queue:pr:tasks")
        processing_len = redis_client.llen("queue:pr:processing")

        curr_round = "Unknown"


        return {
            "workers": worker_count,
            "queue_pending": queue_len,
            "queue_processing": processing_len,
            "redis_alive": True
        }
    except Exception as e:
        return {"error": str(e), "redis_alive": False}

# TODO: have bug for now
@router.post("/scale")
def scale_workers(req: ScaleRequest):
    target = req.count
    if target < 0 or target > 20:
        raise HTTPException(400, "Count must be between 0 and 20")

    try:
        containers = docker_client.containers.list(filters={"ancestor": IMAGE_NAME})
        current_workers = [c for c in containers if "worker.py" in str(c.attrs['Config']['Cmd'])]
        curr_count = len(current_workers)

        if target > curr_count:
            diff = target - curr_count
            for _ in range(diff):
                docker_client.containers.run(
                    image=IMAGE_NAME,
                    command=["python", "compute/pagerank/worker.py"],
                    detach=True,
                    network=NETWORK_NAME,
                    environment=["REDIS_HOST=redis"],
                    volumes={os.getcwd(): {'bind': '/app', 'mode': 'rw'}}
                )
            return {"msg": f"Scaled up by {diff}. Total: {target}"}

        elif target < curr_count:
            diff = curr_count - target
            for i in range(diff):
                current_workers[i].stop()
                current_workers[i].remove()
            return {"msg": f"Scaled down by {diff}. Total: {target}"}

        return {"msg": "No change needed."}

    except Exception as e:
        raise HTTPException(500, str(e))

# TODO: Might have bug? Dont have time to test after backend logic change
@router.post("/trigger/pagerank")
def trigger_pagerank():
    try:
        existing = docker_client.containers.list(filters={"name": "pr-controller"})
        if existing:
            return {"msg": "Controller is already running!"}

        docker_client.containers.run(
            image=IMAGE_NAME,
            name="pr-controller",
            command=["python", "compute/pagerank/controller.py"],
            detach=True,
            network=NETWORK_NAME,
            environment=["REDIS_HOST=redis"],
            volumes={os.getcwd(): {'bind': '/app', 'mode': 'rw'}},
            auto_remove=True
        )
        return {"msg": "PageRank Controller started!"}
    except Exception as e:
        raise HTTPException(500, str(e))