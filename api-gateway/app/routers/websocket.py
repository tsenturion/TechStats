import asyncio
import json
import time
from typing import Any, Dict

import httpx
import structlog
import websockets
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from config import settings
from app.websocket_manager import websocket_manager

router = APIRouter()
logger = structlog.get_logger()


def _to_ws_url(base_url: str) -> str:
    if base_url.startswith("https://"):
        return "wss://" + base_url[len("https://") :]
    if base_url.startswith("http://"):
        return "ws://" + base_url[len("http://") :]
    return base_url


@router.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    await websocket.accept()

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

    backend_url = f"{_to_ws_url(settings.websocket_service_url)}/api/v1/ws/analyze"

    try:
        async with websockets.connect(backend_url, ping_interval=20, ping_timeout=30) as backend_ws:
            await backend_ws.send(json.dumps(payload, ensure_ascii=False))

            while True:
                raw_message = await backend_ws.recv()
                message = json.loads(raw_message)
                await websocket.send_json(message)

                stage = message.get("stage")
                message_type = message.get("type")
                if stage in {"completed", "failed", "error", "cancelled"} or message_type in {"completed", "error"}:
                    break

    except Exception as exc:
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
    except Exception as exc:
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
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}
