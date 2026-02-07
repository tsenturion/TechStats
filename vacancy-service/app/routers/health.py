import asyncio

import httpx
import structlog
from fastapi import APIRouter, HTTPException

from app.cache import cache_manager
from config import settings
from shared.health import build_process_stats, iso_now

router = APIRouter()
logger = structlog.get_logger()


@router.get("/health")
async def health_check():
    health_status = {
        "service": "vacancy-service",
        "status": "healthy",
        "timestamp": iso_now(),
        "version": settings.version,
        "environment": settings.environment,
        "checks": {},
    }

    try:
        if cache_manager.redis_client:
            await cache_manager.redis_client.ping()
            health_status["checks"]["redis"] = {"status": "healthy", "message": "Redis connected"}
        else:
            health_status["checks"]["redis"] = {"status": "unhealthy", "message": "Redis client not initialized"}
            health_status["status"] = "unhealthy"
    except Exception as exc:
        health_status["checks"]["redis"] = {"status": "unhealthy", "message": str(exc)}
        health_status["status"] = "unhealthy"

    try:
        # Проверяем публичный endpoint HH API, который стабильно доступен
        # (корневой "/" часто возвращает 403 и дает ложную деградацию).
        probe_path = "/areas"
        probe_url = f"{settings.hh_api_base_url}{probe_path}"
        probe_headers = {
            "User-Agent": settings.hh_api_user_agent,
            "Accept": "application/json",
            "Accept-Charset": "utf-8",
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(probe_url, headers=probe_headers)
            status_code = response.status_code
            response_time = response.elapsed.total_seconds()

            if 200 <= status_code < 300:
                hh_status = "healthy"
                hh_message = f"HH API accessible via {probe_path}"
            elif status_code in {401, 403, 429}:
                hh_status = "degraded"
                hh_message = f"HH API reachable via {probe_path}, but returned {status_code}"
            else:
                hh_status = "unhealthy"
                hh_message = f"HH API returned {status_code} on {probe_path}"

            health_status["checks"]["hh_api"] = {
                "status": hh_status,
                "message": hh_message,
                "response_time": response_time,
                "probe_endpoint": probe_path,
                "status_code": status_code,
            }

            if hh_status == "unhealthy":
                health_status["status"] = "unhealthy"
            elif hh_status == "degraded" and health_status["status"] == "healthy":
                health_status["status"] = "degraded"
    except Exception as exc:
        health_status["checks"]["hh_api"] = {"status": "unhealthy", "message": str(exc)}
        health_status["status"] = "unhealthy"

    health_status["system"] = build_process_stats()

    try:
        health_status["cache"] = await cache_manager.get_cache_stats()
    except Exception as exc:
        health_status["cache"] = {"error": str(exc)}

    return health_status


@router.get("/health/live")
async def liveness_probe():
    return {"status": "alive", "service": "vacancy-service", "timestamp": iso_now()}


@router.get("/health/detailed")
async def detailed_health_check():
    health_status = await health_check()
    additional_checks = {}

    try:
        if cache_manager.redis_client:
            start_time = asyncio.get_event_loop().time()
            for _ in range(100):
                await cache_manager.redis_client.ping()
            end_time = asyncio.get_event_loop().time()
            additional_checks["redis_speed"] = {
                "status": "healthy",
                "operations_per_second": 100 / (end_time - start_time),
                "latency_ms": (end_time - start_time) * 1000 / 100,
            }
    except Exception as exc:
        additional_checks["redis_speed"] = {"status": "unhealthy", "error": str(exc)}

    endpoints_to_check = ["/vacancies", "/areas", "/industries", "/professional_roles"]
    endpoint_health = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for endpoint in endpoints_to_check:
            try:
                response = await client.get(f"{settings.hh_api_base_url}{endpoint}")
                endpoint_health[endpoint] = {
                    "status": "healthy" if response.status_code == 200 else "unhealthy",
                    "status_code": response.status_code,
                    "response_time": response.elapsed.total_seconds(),
                }
            except Exception as exc:
                endpoint_health[endpoint] = {"status": "unhealthy", "error": str(exc)}

    additional_checks["hh_endpoints"] = endpoint_health
    health_status["detailed_checks"] = additional_checks
    return health_status


@router.get("/health/ready")
async def readiness_probe():
    health = await health_check()
    if health["status"] in {"healthy", "degraded"}:
        return {"status": "ready", "service": "vacancy-service"}
    raise HTTPException(status_code=503, detail="Service not ready")
