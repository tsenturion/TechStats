from fastapi import APIRouter, Request

from app.cache import cache_manager
from config import settings
from shared.health import build_process_stats, iso_now

router = APIRouter()


@router.get("/health")
async def health_check(request: Request):
    payload = {
        "service": "analyzer-service",
        "status": "healthy",
        "timestamp": iso_now(),
        "version": settings.version,
        "environment": settings.environment,
        "checks": {},
    }

    try:
        if cache_manager.redis_client:
            await cache_manager.redis_client.ping()
            payload["checks"]["redis"] = {"status": "healthy", "message": "Redis connected"}
        else:
            payload["checks"]["redis"] = {"status": "unhealthy", "message": "Redis client not initialized"}
            payload["status"] = "unhealthy"
    except Exception as exc:
        payload["checks"]["redis"] = {"status": "unhealthy", "message": str(exc)}
        payload["status"] = "unhealthy"

    try:
        vacancy_client = request.app.state.vacancy_client
        response = await vacancy_client.get("/api/v1/health")
        payload["checks"]["vacancy_service"] = {
            "status": "healthy" if response.status_code == 200 else "degraded",
            "message": "Vacancy service accessible",
            "response_time": response.elapsed.total_seconds(),
        }
        if response.status_code != 200 and payload["status"] == "healthy":
            payload["status"] = "degraded"
    except Exception as exc:
        payload["checks"]["vacancy_service"] = {"status": "unhealthy", "message": str(exc)}
        payload["status"] = "degraded"

    try:
        patterns_loader = request.app.state.patterns_loader
        patterns_count = len(patterns_loader.get_all_patterns())
        payload["checks"]["nlp_tools"] = {
            "status": "healthy",
            "message": f"NLP tools initialized with {patterns_count} patterns",
            "patterns_loaded": patterns_count,
        }
    except Exception as exc:
        payload["checks"]["nlp_tools"] = {"status": "unhealthy", "message": str(exc)}
        payload["status"] = "unhealthy"

    payload["system"] = build_process_stats()

    try:
        payload["cache"] = await cache_manager.get_cache_stats()
    except Exception as exc:
        payload["cache"] = {"error": str(exc)}

    return payload

