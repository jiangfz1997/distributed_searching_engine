import time
import logging
from fastapi import FastAPI, Query
from typing import List
from pydantic import BaseModel
from serving.search_engine import SearchEngine

# 1. 配置日志 (Docker 也能看到)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Search Engine")

# 初始化引擎
# 注意：SearchEngine 内部已经有了重试机制，这里直接实例化即可
engine = SearchEngine()


class SearchResult(BaseModel):
    doc_id: str
    score: float
    detail: str
    snippet: str


@app.get("/search", response_model=List[SearchResult])
def search_api(
        q: str = Query(..., min_length=1, description="Search query"),
        # 2. 增加 limit 参数，允许前端控制返回数量
        limit: int = Query(20, ge=1, le=100, description="Max results to return")
):
    start_time = time.time()

    # 3. 打印请求日志
    logger.info(f"Received query: '{q}' with limit {limit}")

    # 调用引擎 (传入 limit)
    results = engine.search(q, topk=limit)

    duration = time.time() - start_time
    logger.info(f"Query processed in {duration:.4f}s. Found {len(results)} results.")

    return results


@app.get("/healthcheck")
def health_check():
    # 也可以在这里简单查一下库，确保数据库连接是活的
    return {"status": "ok"}