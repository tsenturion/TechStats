# C:\Users\user\Desktop\TechStats\websocket-service\app\routers\websocket_router.py
import asyncio
import time
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
import structlog

from config import settings
from app.connection_manager import ConnectionManager
from app.session_store import SessionStore
from app.analysis_proxy import AnalysisProxy

router = APIRouter()
logger = structlog.get_logger()


async def get_connection_manager(request: Request) -> ConnectionManager:
    """Dependency для получения ConnectionManager"""
    return request.app.state.connection_manager


async def get_session_store(request: Request) -> SessionStore:
    """Dependency для получения SessionStore"""
    return request.app.state.session_store


async def get_analysis_proxy(request: Request) -> AnalysisProxy:
    """Dependency для получения AnalysisProxy"""
    return request.app.state.analysis_proxy


@router.websocket("/ws/analyze")
async def websocket_analyze(
    websocket: WebSocket,
    connection_manager: ConnectionManager = Depends(get_connection_manager),
    analysis_proxy: AnalysisProxy = Depends(get_analysis_proxy)
):
    """
    WebSocket endpoint для анализа с прогрессом в реальном времени
    """
    await connection_manager.connect(websocket)
    
    try:
        # Получение параметров запроса
        data = await websocket.receive_json()
        
        # Запуск анализа
        await analysis_proxy.start_analysis(websocket, data)
        
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected by client")
    except Exception as e:
        logger.error("WebSocket analyze error", error=str(e))
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Server error: {str(e)}"
            })
        except:
            pass
    finally:
        connection_manager.disconnect(websocket)


@router.websocket("/ws/proxy")
async def websocket_proxy(
    websocket: WebSocket,
    connection_manager: ConnectionManager = Depends(get_connection_manager)
):
    """
    WebSocket proxy для перенаправления сообщений между клиентами и анализатором
    """
    await connection_manager.connect(websocket)
    
    try:
        while True:
            # Получение сообщения от клиента
            data = await websocket.receive_json()
            
            # Перенаправление в соответствующий сервис
            message_type = data.get("type")
            
            if message_type == "analyze":
                # Здесь была бы логика перенаправления в analyzer service
                await websocket.send_json({
                    "type": "progress",
                    "stage": "processing",
                    "message": "Анализ начат...",
                    "progress": 10,
                    "timestamp": time.time()
                })
            else:
                await websocket.send_json({
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                })
                
    except WebSocketDisconnect:
        logger.info("WebSocket proxy disconnected")
    except Exception as e:
        logger.error("WebSocket proxy error", error=str(e))
    finally:
        connection_manager.disconnect(websocket)


@router.websocket("/ws/notifications")
async def websocket_notifications(
    websocket: WebSocket,
    connection_manager: ConnectionManager = Depends(get_connection_manager)
):
    """
    WebSocket для получения уведомлений
    """
    await connection_manager.connect(websocket)
    
    try:
        # Подписка на уведомления
        await connection_manager.subscribe(websocket, "notifications")
        
        # Отправка подтверждения
        await websocket.send_json({
            "type": "subscribed",
            "topic": "notifications",
            "timestamp": time.time()
        })
        
        # Ожидание сообщений
        while True:
            try:
                data = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=settings.connection_timeout
                )
                
                # Обработка команд
                if data.get("type") == "unsubscribe":
                    await connection_manager.unsubscribe(websocket, "notifications")
                    await websocket.send_json({
                        "type": "unsubscribed",
                        "topic": "notifications",
                        "timestamp": time.time()
                    })
                    break
                    
            except asyncio.TimeoutException:
                # Отправка ping для поддержания соединения
                try:
                    await websocket.send_json({
                        "type": "ping",
                        "timestamp": time.time()
                    })
                except:
                    break
                
    except WebSocketDisconnect:
        logger.info("Notifications WebSocket disconnected")
    except Exception as e:
        logger.error("Notifications WebSocket error", error=str(e))
    finally:
        await connection_manager.unsubscribe(websocket, "notifications")
        connection_manager.disconnect(websocket)


@router.websocket("/ws/status")
async def websocket_status(
    websocket: WebSocket,
    connection_manager: ConnectionManager = Depends(get_connection_manager),
    session_store: SessionStore = Depends(get_session_store)
):
    """
    WebSocket для получения статуса системы в реальном времени
    """
    await connection_manager.connect(websocket)
    
    try:
        # Подписка на обновления статуса
        await connection_manager.subscribe(websocket, "system_status")
        
        # Отправка начального статуса
        initial_status = {
            "type": "status_update",
            "timestamp": time.time(),
            "data": {
                "connections": connection_manager.active_connections_count(),
                "sessions": await session_store.get_session_stats(),
                "system": {
                    "uptime": asyncio.get_event_loop().time(),
                    "memory": "healthy",
                    "cpu": "normal"
                }
            }
        }
        
        await websocket.send_json(initial_status)
        
        # Периодическая отправка обновлений
        update_count = 0
        while True:
            await asyncio.sleep(5)  # Обновляем каждые 5 секунд
            
            status_update = {
                "type": "status_update",
                "timestamp": time.time(),
                "data": {
                    "connections": connection_manager.active_connections_count(),
                    "sessions": await session_store.get_session_stats(),
                    "update_count": update_count
                }
            }
            
            await websocket.send_json(status_update)
            update_count += 1
            
    except WebSocketDisconnect:
        logger.info("Status WebSocket disconnected")
    except Exception as e:
        logger.error("Status WebSocket error", error=str(e))
    finally:
        await connection_manager.unsubscribe(websocket, "system_status")
        connection_manager.disconnect(websocket)


@router.get("/ws/sessions")
async def get_active_sessions(
    session_store: SessionStore = Depends(get_session_store),
    limit: int = 20,
    offset: int = 0
):
    """Получение активных сессий"""
    try:
        sessions = await session_store.get_active_sessions(limit, offset)
        
        return {
            "sessions": sessions,
            "total": len(sessions),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error("Failed to get active sessions", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ws/sessions/{session_id}")
async def get_session(
    session_id: str,
    session_store: SessionStore = Depends(get_session_store)
):
    """Получение информации о сессии"""
    try:
        session = await session_store.get_session(session_id)
        
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        return session
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get session", session_id=session_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ws/sessions/{session_id}/cancel")
async def cancel_session(
    session_id: str,
    analysis_proxy: AnalysisProxy = Depends(get_analysis_proxy),
    session_store: SessionStore = Depends(get_session_store)
):
    """Отмена сессии анализа"""
    try:
        # Проверка существования сессии
        session = await session_store.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Попытка отмены
        cancelled = await analysis_proxy.cancel_analysis(session_id)
        
        if cancelled:
            return {
                "success": True,
                "message": f"Session {session_id} cancelled",
                "session_id": session_id
            }
        else:
            return {
                "success": False,
                "message": f"Session {session_id} not found or already completed",
                "session_id": session_id
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to cancel session", session_id=session_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/ws/sessions/{session_id}")
async def delete_session(
    session_id: str,
    session_store: SessionStore = Depends(get_session_store)
):
    """Удаление сессии"""
    try:
        deleted = await session_store.delete_session(session_id)
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Session not found")
        
        return {
            "success": True,
            "message": f"Session {session_id} deleted",
            "session_id": session_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete session", session_id=session_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ws/connections")
async def get_connections(
    connection_manager: ConnectionManager = Depends(get_connection_manager)
):
    """Получение информации о подключениях"""
    try:
        stats = connection_manager.get_connection_stats()
        
        return {
            "connections": stats,
            "active_count": connection_manager.active_connections_count(),
            "total_accepted": connection_manager.total_connections_accepted(),
            "total_rejected": connection_manager.total_connections_rejected()
        }
        
    except Exception as e:
        logger.error("Failed to get connections", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ws/broadcast")
async def broadcast_message(
    request: Request,
    broadcast_data: Dict[str, Any],
    connection_manager: ConnectionManager = Depends(get_connection_manager)
):
    """Широковещательная рассылка сообщения"""
    try:
        message = broadcast_data.get("message", {})
        topic = broadcast_data.get("topic")
        exclude = broadcast_data.get("exclude", [])
        
        if not message:
            raise HTTPException(status_code=400, detail="Message is required")
        
        # Добавление метаданных
        message.update({
            "broadcasted_at": time.time(),
            "broadcast_id": f"broadcast_{int(time.time())}"
        })
        
        # Отправка
        if topic:
            results = await connection_manager.broadcast_to_topic(topic, message)
            target = f"topic '{topic}'"
        else:
            results = await connection_manager.broadcast(message, exclude)
            target = "all connections"
        
        # Подсчет успешных отправок
        successful = sum(1 for _, success in results if success)
        failed = len(results) - successful
        
        return {
            "success": True,
            "message": f"Broadcast to {target} completed",
            "stats": {
                "total_recipients": len(results),
                "successful": successful,
                "failed": failed,
                "target": target
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to broadcast message", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))