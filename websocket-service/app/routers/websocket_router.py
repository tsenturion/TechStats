import asyncio
import time
from typing import Any, Dict

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect

from app.analysis_proxy import AnalysisProxy
from app.connection_manager import ConnectionManager
from app.session_store import SessionStore
from config import settings

router = APIRouter()
logger = structlog.get_logger()


def _cm_from_ws(websocket: WebSocket) -> ConnectionManager:
    return websocket.app.state.connection_manager


def _store_from_ws(websocket: WebSocket) -> SessionStore:
    return websocket.app.state.session_store


def _proxy_from_ws(websocket: WebSocket) -> AnalysisProxy:
    return websocket.app.state.analysis_proxy


async def get_connection_manager(request: Request) -> ConnectionManager:
    return request.app.state.connection_manager


async def get_session_store(request: Request) -> SessionStore:
    return request.app.state.session_store


async def get_analysis_proxy(request: Request) -> AnalysisProxy:
    return request.app.state.analysis_proxy


@router.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    connection_manager = _cm_from_ws(websocket)
    analysis_proxy = _proxy_from_ws(websocket)
    await connection_manager.connect(websocket)
    try:
        payload = await websocket.receive_json()
        await analysis_proxy.start_analysis(websocket, payload)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected by client")
    except Exception as exc:
        logger.error("WebSocket analyze error", error=str(exc))
        try:
            await websocket.send_json({"type": "error", "message": f"Server error: {str(exc)}"})
        except Exception:
            pass
    finally:
        await connection_manager.disconnect(websocket)


@router.websocket("/ws/proxy")
async def websocket_proxy(websocket: WebSocket):
    connection_manager = _cm_from_ws(websocket)
    analysis_proxy = _proxy_from_ws(websocket)
    await connection_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            message_type = data.get("type")
            if message_type == "analyze":
                await analysis_proxy.start_analysis(websocket, data)
            else:
                await websocket.send_json({"type": "error", "message": f"Unknown message type: {message_type}"})
    except WebSocketDisconnect:
        logger.info("WebSocket proxy disconnected")
    except Exception as exc:
        logger.error("WebSocket proxy error", error=str(exc))
    finally:
        await connection_manager.disconnect(websocket)


@router.websocket("/ws/notifications")
async def websocket_notifications(websocket: WebSocket):
    connection_manager = _cm_from_ws(websocket)
    await connection_manager.connect(websocket)
    try:
        await connection_manager.subscribe(websocket, "notifications")
        await websocket.send_json({"type": "subscribed", "topic": "notifications", "timestamp": time.time()})
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=settings.connection_timeout)
                if data.get("type") == "unsubscribe":
                    await connection_manager.unsubscribe(websocket, "notifications")
                    await websocket.send_json({"type": "unsubscribed", "topic": "notifications", "timestamp": time.time()})
                    break
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping", "timestamp": time.time()})
                except Exception:
                    break
    except WebSocketDisconnect:
        logger.info("Notifications WebSocket disconnected")
    except Exception as exc:
        logger.error("Notifications WebSocket error", error=str(exc))
    finally:
        await connection_manager.unsubscribe(websocket, "notifications")
        await connection_manager.disconnect(websocket)


@router.websocket("/ws/status")
async def websocket_status(websocket: WebSocket):
    connection_manager = _cm_from_ws(websocket)
    session_store = _store_from_ws(websocket)
    await connection_manager.connect(websocket)
    try:
        await connection_manager.subscribe(websocket, "system_status")
        await websocket.send_json(
            {
                "type": "status_update",
                "timestamp": time.time(),
                "data": {
                    "connections": connection_manager.active_connections_count(),
                    "sessions": await session_store.get_session_stats(),
                },
            }
        )

        update_count = 0
        while True:
            await asyncio.sleep(5)
            await websocket.send_json(
                {
                    "type": "status_update",
                    "timestamp": time.time(),
                    "data": {
                        "connections": connection_manager.active_connections_count(),
                        "sessions": await session_store.get_session_stats(),
                        "update_count": update_count,
                    },
                }
            )
            update_count += 1
    except WebSocketDisconnect:
        logger.info("Status WebSocket disconnected")
    except Exception as exc:
        logger.error("Status WebSocket error", error=str(exc))
    finally:
        await connection_manager.unsubscribe(websocket, "system_status")
        await connection_manager.disconnect(websocket)


@router.get("/ws/sessions")
async def get_active_sessions(session_store: SessionStore = Depends(get_session_store), limit: int = 20, offset: int = 0):
    try:
        sessions = await session_store.get_active_sessions(limit, offset)
        return {"sessions": sessions, "total": len(sessions), "limit": limit, "offset": offset}
    except Exception as exc:
        logger.error("Failed to get active sessions", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/ws/sessions/{session_id}")
async def get_session(session_id: str, session_store: SessionStore = Depends(get_session_store)):
    try:
        session = await session_store.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        return session
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get session", session_id=session_id, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/ws/sessions/{session_id}/cancel")
async def cancel_session(
    session_id: str,
    analysis_proxy: AnalysisProxy = Depends(get_analysis_proxy),
    session_store: SessionStore = Depends(get_session_store),
):
    try:
        if not await session_store.get_session(session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        cancelled = await analysis_proxy.cancel_analysis(session_id)
        if cancelled:
            return {"success": True, "message": f"Session {session_id} cancelled", "session_id": session_id}
        return {
            "success": False,
            "message": f"Session {session_id} not found or already completed",
            "session_id": session_id,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to cancel session", session_id=session_id, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/ws/sessions/{session_id}")
async def delete_session(session_id: str, session_store: SessionStore = Depends(get_session_store)):
    try:
        deleted = await session_store.delete_session(session_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Session not found")
        return {"success": True, "message": f"Session {session_id} deleted", "session_id": session_id}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to delete session", session_id=session_id, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/ws/connections")
async def get_connections(connection_manager: ConnectionManager = Depends(get_connection_manager)):
    try:
        stats = connection_manager.get_connection_stats()
        return {
            "connections": stats,
            "active_count": connection_manager.active_connections_count(),
            "total_accepted": connection_manager.total_connections_accepted(),
            "total_rejected": connection_manager.total_connections_rejected(),
        }
    except Exception as exc:
        logger.error("Failed to get connections", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/ws/broadcast")
async def broadcast_message(
    request: Request,
    broadcast_data: Dict[str, Any],
    connection_manager: ConnectionManager = Depends(get_connection_manager),
):
    try:
        message = broadcast_data.get("message", {})
        topic = broadcast_data.get("topic")
        exclude = broadcast_data.get("exclude", [])
        if not message:
            raise HTTPException(status_code=400, detail="Message is required")

        message.update({"broadcasted_at": time.time(), "broadcast_id": f"broadcast_{int(time.time())}"})
        if topic:
            results = await connection_manager.broadcast_to_topic(topic, message)
            target = f"topic '{topic}'"
        else:
            results = await connection_manager.broadcast(message, exclude)
            target = "all connections"

        successful = sum(1 for _, success in results if success)
        failed = len(results) - successful
        return {
            "success": True,
            "message": f"Broadcast to {target} completed",
            "stats": {
                "total_recipients": len(results),
                "successful": successful,
                "failed": failed,
                "target": target,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to broadcast message", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))

