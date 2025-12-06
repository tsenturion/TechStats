# C:\Users\user\Desktop\TechStats\websocket-service\app\routers\admin.py
import asyncio
import time
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends, Request, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import structlog

from config import settings
from app.connection_manager import ConnectionManager
from app.session_store import SessionStore
from app.analysis_proxy import AnalysisProxy

router = APIRouter()
security = HTTPBearer()
logger = structlog.get_logger()


async def verify_admin_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Проверка токена администратора"""
    # В production здесь была бы полноценная проверка JWT
    admin_token = "admin_secret_token"  # Должен быть в настройках
    
    if credentials.credentials != admin_token:
        raise HTTPException(
            status_code=403,
            detail="Invalid admin token"
        )
    
    return True


@router.get("/connections")
async def admin_get_connections(
    request: Request,
    detailed: bool = Query(False),
    limit: int = Query(50),
    offset: int = Query(0),
    _: bool = Depends(verify_admin_token)
):
    """Административный endpoint для получения информации о соединениях"""
    try:
        connection_manager = request.app.state.connection_manager
        
        if detailed:
            # Детальная информация о всех соединениях
            connection_info = []
            
            for conn_id, info in connection_manager.connection_info.items():
                connection_info.append({
                    "id": conn_id,
                    **info
                })
            
            # Применение пагинации
            connection_info = connection_info[offset:offset + limit]
            
            return {
                "connections": connection_info,
                "total": len(connection_manager.connection_info),
                "limit": limit,
                "offset": offset
            }
        else:
            # Только статистика
            stats = connection_manager.get_connection_stats()
            
            return {
                "stats": stats,
                "active_count": connection_manager.active_connections_count(),
                "total_accepted": connection_manager.total_connections_accepted(),
                "total_rejected": connection_manager.total_connections_rejected()
            }
        
    except Exception as e:
        logger.error("Admin failed to get connections", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connections/{connection_id}")
async def admin_get_connection(
    connection_id: str,
    request: Request,
    include_history: bool = Query(False),
    history_limit: int = Query(20),
    _: bool = Depends(verify_admin_token)
):
    """Получение информации о конкретном соединении"""
    try:
        connection_manager = request.app.state.connection_manager
        
        # Получение информации о соединении
        info = connection_manager.get_connection_info(connection_id)
        if not info:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        result = {
            "id": connection_id,
            **info
        }
        
        if include_history:
            # Получение истории сообщений
            history = connection_manager.get_message_history(connection_id, history_limit)
            result["message_history"] = history
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Admin failed to get connection", connection_id=connection_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/connections/{connection_id}")
async def admin_disconnect_connection(
    connection_id: str,
    request: Request,
    _: bool = Depends(verify_admin_token)
):
    """Принудительное отключение соединения"""
    try:
        connection_manager = request.app.state.connection_manager
        
        # Поиск WebSocket по connection_id
        websocket = connection_manager.active_connections.get(connection_id)
        if not websocket:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        # Закрытие соединения
        try:
            await websocket.close(
                code=1000,
                reason="Disconnected by administrator"
            )
        except:
            pass
        
        # Удаление из менеджера
        connection_manager.disconnect(websocket)
        
        logger.warning("Connection disconnected by admin", connection_id=connection_id)
        
        return {
            "success": True,
            "message": f"Connection {connection_id} disconnected",
            "connection_id": connection_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Admin failed to disconnect connection", connection_id=connection_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sessions")
async def admin_get_sessions(
    request: Request,
    status: Optional[str] = Query(None),
    stage: Optional[str] = Query(None),
    min_progress: Optional[float] = Query(None),
    max_progress: Optional[float] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
    _: bool = Depends(verify_admin_token)
):
    """Получение сессий с фильтрацией"""
    try:
        session_store = request.app.state.session_store
        
        # Построение запроса
        query = {}
        
        if status:
            query["status"] = status
        
        if stage:
            query["stage"] = stage
        
        if min_progress is not None:
            query["min_progress"] = min_progress
        
        if max_progress is not None:
            query["max_progress"] = max_progress
        
        # Поиск сессий
        sessions = await session_store.search_sessions(query, limit=limit)
        
        # Применение пагинации
        sessions = sessions[offset:offset + limit]
        
        return {
            "sessions": sessions,
            "total_found": len(sessions),
            "limit": limit,
            "offset": offset,
            "query": query
        }
        
    except Exception as e:
        logger.error("Admin failed to get sessions", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sessions/stats")
async def admin_get_session_stats(
    request: Request,
    hours: int = Query(24),
    _: bool = Depends(verify_admin_token)
):
    """Получение статистики сессий"""
    try:
        session_store = request.app.state.session_store
        
        # Базовая статистика
        stats = await session_store.get_session_stats()
        
        # Дополнительная аналитика за период
        now = time.time()
        start_time = now - (hours * 3600)
        
        # Получение сессий за период
        recent_sessions = await session_store.search_sessions({
            "created_after": start_time
        }, limit=1000)
        
        # Анализ по часам
        hourly_counts = {}
        for session in recent_sessions:
            created_at = session.get("created_at", now)
            hour = int((created_at - start_time) / 3600)
            hourly_counts[hour] = hourly_counts.get(hour, 0) + 1
        
        # Расчет успешности
        completed = sum(1 for s in recent_sessions if s.get("status") == "completed")
        failed = sum(1 for s in recent_sessions if s.get("status") == "failed")
        total = len(recent_sessions)
        
        success_rate = (completed / total * 100) if total > 0 else 0
        
        stats["recent_period"] = {
            "hours": hours,
            "total_sessions": total,
            "completed": completed,
            "failed": failed,
            "success_rate": success_rate,
            "hourly_distribution": [
                {"hour": hour, "count": count}
                for hour, count in sorted(hourly_counts.items())
            ]
        }
        
        return stats
        
    except Exception as e:
        logger.error("Admin failed to get session stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/system/cleanup")
async def admin_run_cleanup(
    request: Request,
    cleanup_type: str = "all",
    _: bool = Depends(verify_admin_token)
):
    """Запуск очистки системы"""
    try:
        session_store = request.app.state.session_store
        connection_manager = request.app.state.connection_manager
        
        results = {}
        
        if cleanup_type in ["sessions", "all"]:
            # Очистка сессий
            cleaned_sessions = await session_store.cleanup_expired_sessions()
            results["sessions_cleaned"] = cleaned_sessions
        
        if cleanup_type in ["connections", "all"]:
            # Очистка соединений
            await connection_manager.cleanup_inactive_connections()
            results["connections_cleaned"] = "inactive connections removed"
        
        if cleanup_type in ["analyses", "all"]:
            # Очистка анализов
            analysis_proxy = request.app.state.analysis_proxy
            cancelled_analyses = await analysis_proxy.cleanup_cancelled_analyses()
            results["analyses_cleaned"] = cancelled_analyses
        
        return {
            "success": True,
            "message": f"Cleanup completed for {cleanup_type}",
            "results": results,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Admin failed to run cleanup", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/info")
async def admin_get_system_info(
    request: Request,
    _: bool = Depends(verify_admin_token)
):
    """Получение полной информации о системе"""
    try:
        connection_manager = request.app.state.connection_manager
        session_store = request.app.state.session_store
        analysis_proxy = request.app.state.analysis_proxy
        
        # Информация о соединениях
        connection_stats = connection_manager.get_connection_stats()
        
        # Информация о сессиях
        session_stats = await session_store.get_session_stats()
        
        # Информация об анализах
        active_analyses = await analysis_proxy.get_active_analysis_count()
        
        # Системная информация
        import psutil
        import os
        
        process = psutil.Process()
        memory_info = process.memory_info()
        
        system_info = {
            "process": {
                "pid": os.getpid(),
                "memory_usage_mb": memory_info.rss / 1024 / 1024,
                "cpu_percent": process.cpu_percent(),
                "threads": process.num_threads(),
                "uptime": asyncio.get_event_loop().time()
            },
            "python": {
                "version": os.sys.version,
                "implementation": os.sys.implementation.name
            },
            "settings": {
                "max_connections": settings.max_total_connections,
                "max_connections_per_ip": settings.max_connections_per_ip,
                "connection_timeout": settings.connection_timeout,
                "session_ttl": settings.session_ttl_seconds,
                "ping_interval": settings.websocket_ping_interval
            }
        }
        
        return {
            "connections": connection_stats,
            "sessions": session_stats,
            "analyses": {
                "active": active_analyses,
                "recent_completed": session_stats.get("completed", 0),
                "recent_failed": session_stats.get("failed", 0)
            },
            "system": system_info,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Admin failed to get system info", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))