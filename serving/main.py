import time
import logging
from fastapi import FastAPI, Query
from typing import List
from pydantic import BaseModel
from serving.search_engine import SearchEngine
from serving.admin import router as admin_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Search Engine")
app.include_router(admin_router)

engine = SearchEngine()


class SearchResult(BaseModel):
    doc_id: str
    score: float
    detail: str
    snippet: str


@app.get("/search", response_model=List[SearchResult])
def search_api(
        q: str = Query(..., min_length=1, description="Search query"),
        limit: int = Query(20, ge=1, le=100, description="Max results to return"),
        pagerank: bool = Query(True, description="Whether to use PageRank for ranking"),
        semantics: bool = Query(False, description="Whether to use semantic search"),
        alpha: float = Query(None, ge=0.0, le=1.0, description="Balance between semantic and lexical search"),
        beta: float = Query(None, ge=0.0, le=1.0, description="Weight for PageRank in final scoring")

):
    start_time = time.time()

    logger.info(f"Received query: '{q}' with limit {limit}")

    results = engine.search(q, topk=limit, pagerank=pagerank, use_semantics=semantics, alpha=alpha, beta=beta)

    duration = time.time() - start_time
    logger.info(f"Query processed in {duration:.4f}s. Found {len(results)} results.")

    return results


@app.get("/healthcheck")
def health_check():
    return {"status": "ok"}