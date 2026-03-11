import time

import httpx
from fastapi import APIRouter
from fastapi_health import health as health_route

from config import settings

router = APIRouter()


@router.get("/health")
async def gateway_health():
    return {
        "service": "api-gateway",
        "status": "healthy",
        "timestamp": time.time(),
    }


@router.get("/health/services")
async def services_health():
    services = {
        "vacancy": settings.vacancy_service_url,
        "analyzer": settings.analyzer_service_url,
        "cache": settings.cache_service_url,
        "websocket": settings.websocket_service_url,
    }
    results = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, base_url in services.items():
            try:
                response = await client.get(f"{base_url}/api/v1/health")
                results[name] = {
                    "status_code": response.status_code,
                    "healthy": response.status_code == 200,
                }
            except Exception as exc:
                results[name] = {"healthy": False, "error": str(exc)}
    return {"services": results}


async def _gateway_ready() -> bool:
    return True


router.add_api_route("/health/live", health_route([_gateway_ready]), methods=["GET"])
