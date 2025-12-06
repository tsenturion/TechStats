# C:\Users\user\Desktop\TechStats\cache-service\app\routers\metrics.py
import time
from typing import Dict, Any
from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Histogram, Gauge

router = APIRouter()

# Prometheus метрики
CACHE_OPERATIONS = Counter(
    'cache_operations_total',
    'Total cache operations',
    ['operation', 'node']
)
CACHE_HITS = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['node']
)
CACHE_MISSES = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['node']
)
CACHE_SIZE = Gauge(
    'cache_size_items',
    'Number of items in cache',
    ['node']
)
CACHE_MEMORY = Gauge(
    'cache_memory_bytes',
    'Memory used by cache',
    ['node']
)
REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration',
    ['method', 'endpoint', 'node']
)


@router.get("/metrics")
async def get_metrics(request: Request):
    """
    Prometheus метрики
    """
    # Обновляем метрики на основе текущего состояния
    if hasattr(request.app.state, 'cache_manager'):
        try:
            stats = await request.app.state.cache_manager.get_stats()
            
            if "backend" in stats and "total_items" in stats["backend"]:
                CACHE_SIZE.labels(node=request.app.state.settings.node_id).set(
                    stats["backend"]["total_items"]
                )
            
            if "backend" in stats and "used_memory_human" in stats["backend"]:
                # Парсим human-readable формат
                memory_str = stats["backend"]["used_memory_human"]
                if "MB" in memory_str:
                    memory_mb = float(memory_str.replace("MB", "").strip())
                    memory_bytes = memory_mb * 1024 * 1024
                    CACHE_MEMORY.labels(node=request.app.state.settings.node_id).set(memory_bytes)
        
        except Exception as e:
            pass
    
    metrics = generate_latest()
    return Response(
        content=metrics,
        media_type=CONTENT_TYPE_LATEST,
        headers={"Cache-Control": "no-cache"}
    )