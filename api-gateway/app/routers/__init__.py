from .analyzer import router as analyzer_router
from .cache import router as cache_router
from .health import router as health_router
from .vacancy import router as vacancy_router
from .websocket import router as websocket_router

__all__ = [
    "analyzer_router",
    "cache_router",
    "health_router",
    "vacancy_router",
    "websocket_router",
]
