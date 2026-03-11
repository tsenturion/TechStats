from .analyzer import router as analyzer_router
from .auth import router as auth_router
from .cache import router as cache_router
from .health import router as health_router
from .runtime_settings import router as runtime_settings_router
from .vacancy import router as vacancy_router
from .websocket import router as websocket_router

__all__ = [
    "analyzer_router",
    "auth_router",
    "cache_router",
    "health_router",
    "runtime_settings_router",
    "vacancy_router",
    "websocket_router",
]
