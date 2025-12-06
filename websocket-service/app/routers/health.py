# C:\Users\user\Desktop\TechStats\websocket-service\app\routers\health.py
import asyncio
from datetime import datetime
from typing import Dict, Any
import httpx
from fastapi import APIRouter, HTTPException, Depends, Request
import structlog

from config import settings
from app.connection_manager import ConnectionManager
from app.session_store import SessionStore
from app.analysis_proxy import AnalysisProxy

router = APIRouter()
logger = structlog.get_logger()


@router.get("/health")
async def health_check(request: Request):
    """
    Проверка здоровья сервиса
    """
    health_status = {
        "service": "websocket-service",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": settings.version,
        "environment": settings.environment,
        "checks": {}
    }
    
    # Проверка Redis
    try:
        redis_client = request.app.state.redis_client
        await redis_client.ping()
        health_status["checks"]["redis"] = {
            "status": "healthy",
            "message": "Redis connected"
        }
    except Exception as e:
        health_status["checks"]["redis"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "unhealthy"
    
    # Проверка Analyzer Service
    try:
        analyzer_client = request.app.state.analyzer_client
        response = await analyzer_client.get("/api/v1/health")
        
        if response.status_code == 200:
            analyzer_health = response.json()
            health_status["checks"]["analyzer_service"] = {
                "status": analyzer_health.get("status", "unknown"),
                "message": "Analyzer service accessible",
                "response_time": response.elapsed.total_seconds()
            }
            
            if analyzer_health.get("status") != "healthy":
                health_status["status"] = "degraded"
        else:
            health_status["checks"]["analyzer_service"] = {
                "status": "unhealthy",
                "message": f"Analyzer service returned {response.status_code}",
                "response_time": response.elapsed.total_seconds()
            }
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["analyzer_service"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "degraded"
    
    # Проверка Vacancy Service
    try:
        vacancy_client = request.app.state.vacancy_client
        response = await vacancy_client.get("/api/v1/health")
        
        if response.status_code == 200:
            vacancy_health = response.json()
            health_status["checks"]["vacancy_service"] = {
                "status": vacancy_health.get("status", "unknown"),
                "message": "Vacancy service accessible",
                "response_time": response.elapsed.total_seconds()
            }
        else:
            health_status["checks"]["vacancy_service"] = {
                "status": "unhealthy",
                "message": f"Vacancy service returned {response.status_code}",
                "response_time": response.elapsed.total_seconds()
            }
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["vacancy_service"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "degraded"
    
    # Проверка Connection Manager
    try:
        connection_manager = request.app.state.connection_manager
        active_connections = connection_manager.active_connections_count()
        
        health_status["checks"]["connection_manager"] = {
            "status": "healthy",
            "message": f"Connection manager active with {active_connections} connections",
            "active_connections": active_connections,
            "total_accepted": connection_manager.total_connections_accepted(),
            "total_rejected": connection_manager.total_connections_rejected()
        }
        
        # Проверка перегрузки
        if active_connections >= settings.max_total_connections * 0.9:
            health_status["checks"]["connection_manager"]["status"] = "warning"
            health_status["checks"]["connection_manager"]["message"] = "High connection count"
            
    except Exception as e:
        health_status["checks"]["connection_manager"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "unhealthy"
    
    # Проверка Session Store
    try:
        session_store = request.app.state.session_store
        session_stats = await session_store.get_session_stats()
        
        health_status["checks"]["session_store"] = {
            "status": "healthy",
            "message": f"Session store with {session_stats.get('total_sessions', 0)} sessions",
            "session_stats": session_stats
        }
    except Exception as e:
        health_status["checks"]["session_store"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "unhealthy"
    
    # Проверка памяти
    import psutil
    process = psutil.Process()
    memory_info = process.memory_info()
    
    health_status["system"] = {
        "memory_usage_mb": memory_info.rss / 1024 / 1024,
        "cpu_percent": process.cpu_percent(),
        "threads": process.num_threads(),
        "uptime": asyncio.get_event_loop().time()
    }
    
    return health_status