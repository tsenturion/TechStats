import asyncio
import json
import time
from typing import Any, Dict

import httpx
import structlog
import websockets
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.runtime_config import get_runtime_settings_effective
from app.security import UserRole, decode_access_token
from app.websocket_manager import websocket_manager
from config import settings

router = APIRouter()
logger = structlog.get_logger()


def _to_ws_url(base_url: str) -> str:
    if base_url.startswith("https://"):
        return "wss://" + base_url[len("https://") :]
    if base_url.startswith("http://"):
        return "ws://" + base_url[len("http://") :]
    return base_url


def _extract_bearer_token_from_header(raw_header: str) -> str:
    if not raw_header:
        return ""
    try:
        scheme, token = raw_header.split(" ", 1)
    except ValueError:
        return ""
    if scheme.lower() != "bearer":
        return ""
    return token.strip()


def _extract_ws_access_token(websocket: WebSocket) -> str:
    query_token = websocket.query_params.get("access_token", "").strip()
    if query_token:
        return query_token
    header_token = _extract_bearer_token_from_header(websocket.headers.get("Authorization", ""))
    return header_token


def _normalize_ws_payload(payload: Dict[str, Any], runtime_settings: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(payload)
    max_pages_limit = int(runtime_settings.get("analysis_max_pages_hard_limit", 20))
    per_page_limit = int(runtime_settings.get("analysis_per_page_hard_limit", 100))

    if "max_pages" in normalized:
        normalized["max_pages"] = max(1, min(int(normalized.get("max_pages", 1)), max_pages_limit))
    else:
        normalized["max_pages"] = int(runtime_settings.get("search_default_max_pages", 3))

    if "per_page" in normalized:
        normalized["per_page"] = max(1, min(int(normalized.get("per_page", 1)), per_page_limit))
    else:
        normalized["per_page"] = int(runtime_settings.get("search_default_per_page", 50))

    if "exact_search" not in normalized:
        normalized["exact_search"] = bool(runtime_settings.get("search_default_exact", True))
    if "area" not in normalized:
        normalized["area"] = int(runtime_settings.get("search_default_area", 113))
    if "use_cache" not in normalized:
        normalized["use_cache"] = bool(runtime_settings.get("analysis_default_use_cache", True))

    return normalized


def _assert_ws_user_role(token: str) -> Dict[str, Any]:
    if not token:
        raise ValueError("Authentication required")
    payload = decode_access_token(token)
    if payload.get("role") not in {UserRole.user.value, UserRole.admin.value}:
        raise ValueError("User role required")
    return payload


@router.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    await websocket.accept()

    token = _extract_ws_access_token(websocket)
    try:
        _assert_ws_user_role(token)
    except Exception as exc:  # noqa: BLE001
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close(code=4401)
        return

    try:
        payload = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    except Exception:
        await websocket.send_json({"type": "error", "message": "Invalid JSON payload"})
        await websocket.close(code=1003)
        return

    required_fields = ("vacancy_title", "technology")
    for field in required_fields:
        if field not in payload:
            await websocket.send_json({"type": "error", "message": f"Missing required field: {field}"})
            await websocket.close(code=1008)
            return

    runtime_settings = await get_runtime_settings_effective()
    payload = _normalize_ws_payload(payload, runtime_settings)
    request_delay_ms = int(runtime_settings.get("gateway_analyzer_request_delay_ms", 0))
    if request_delay_ms > 0:
        await asyncio.sleep(request_delay_ms / 1000.0)

    backend_url = f"{_to_ws_url(settings.websocket_service_url)}/api/v1/ws/analyze"

    try:
        async with websockets.connect(
            backend_url,
            ping_interval=settings.websocket_ping_interval,
            ping_timeout=settings.websocket_ping_timeout,
        ) as backend_ws:
            await backend_ws.send(json.dumps(payload, ensure_ascii=False))

            while True:
                raw_message = await backend_ws.recv()
                message = json.loads(raw_message)
                await websocket.send_json(message)

                stage = message.get("stage")
                message_type = message.get("type")
                if stage in {"completed", "failed", "error", "cancelled"} or message_type in {"completed", "error"}:
                    break

    except Exception as exc:  # noqa: BLE001
        logger.error("WebSocket proxy error", error=str(exc))
        await websocket.send_json({"type": "error", "message": f"WebSocket proxy error: {str(exc)}"})
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


@router.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    await websocket_manager.connect(websocket)

    try:
        while True:
            data = {
                "connections": websocket_manager.active_connections_count(),
                "timestamp": time.time(),
                "services": {
                    "vacancy": await check_service_health(settings.vacancy_service_url),
                    "analyzer": await check_service_health(settings.analyzer_service_url),
                    "cache": await check_service_health(settings.cache_service_url),
                    "websocket": await check_service_health(settings.websocket_service_url),
                },
            }
            await websocket.send_json({"type": "metrics", "data": data})
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.error("Metrics WebSocket error", error=str(exc))
    finally:
        await websocket_manager.disconnect(websocket)
        try:
            await websocket.close()
        except Exception:
            pass


async def check_service_health(url: str) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{url}/api/v1/health")
            return {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "response_time": response.elapsed.total_seconds(),
                "status_code": response.status_code,
            }
    except Exception as exc:  # noqa: BLE001
        return {"status": "unavailable", "error": str(exc)}
