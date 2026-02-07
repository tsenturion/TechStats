from fastapi import APIRouter, Request

from config import settings
from shared.health import build_process_stats, iso_now

router = APIRouter()


@router.get("/health")
async def health_check(request: Request):
    payload = {
        "service": "websocket-service",
        "status": "healthy",
        "timestamp": iso_now(),
        "version": settings.version,
        "environment": settings.environment,
        "checks": {},
    }

    try:
        redis_client = request.app.state.redis_client
        await redis_client.ping()
        payload["checks"]["redis"] = {"status": "healthy", "message": "Redis connected"}
    except Exception as exc:
        payload["checks"]["redis"] = {"status": "unhealthy", "message": str(exc)}
        payload["status"] = "unhealthy"

    for service_name, client_name in (
        ("analyzer_service", "analyzer_client"),
        ("vacancy_service", "vacancy_client"),
    ):
        try:
            client = getattr(request.app.state, client_name)
            response = await client.get("/api/v1/health")
            payload["checks"][service_name] = {
                "status": "healthy" if response.status_code == 200 else "degraded",
                "response_time": response.elapsed.total_seconds(),
            }
            if response.status_code != 200 and payload["status"] == "healthy":
                payload["status"] = "degraded"
        except Exception as exc:
            payload["checks"][service_name] = {"status": "unhealthy", "message": str(exc)}
            payload["status"] = "degraded"

    try:
        connection_manager = request.app.state.connection_manager
        payload["checks"]["connection_manager"] = {
            "status": "healthy",
            "active_connections": connection_manager.active_connections_count(),
            "total_accepted": connection_manager.total_connections_accepted(),
            "total_rejected": connection_manager.total_connections_rejected(),
        }
    except Exception as exc:
        payload["checks"]["connection_manager"] = {"status": "unhealthy", "message": str(exc)}
        payload["status"] = "unhealthy"

    try:
        session_store = request.app.state.session_store
        payload["checks"]["session_store"] = {
            "status": "healthy",
            "session_stats": await session_store.get_session_stats(),
        }
    except Exception as exc:
        payload["checks"]["session_store"] = {"status": "unhealthy", "message": str(exc)}
        payload["status"] = "unhealthy"

    payload["system"] = build_process_stats()
    return payload

