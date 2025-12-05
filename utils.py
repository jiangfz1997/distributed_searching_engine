import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            end = time.perf_counter()
            duration = end - start
            logger.info(f"PERF: {func.__name__} finished in {duration:.4f}s")
    return wrapper