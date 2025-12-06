# C:\Users\user\Desktop\TechStats\analyzer-service\app\routers\health.py
import asyncio
from datetime import datetime
from typing import Dict, Any
import httpx
from fastapi import APIRouter, HTTPException, Depends, Request
import structlog

from config import settings
from app.cache import cache_manager

router = APIRouter()
logger = structlog.get_logger()


@router.get("/health")
async def health_check(request: Request):
    """
    Проверка здоровья сервиса
    """
    health_status = {
        "service": "analyzer-service",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": settings.version,
        "environment": settings.environment,
        "checks": {}
    }
    
    # Проверка Redis
    try:
        if cache_manager.redis_client:
            await cache_manager.redis_client.ping()
            health_status["checks"]["redis"] = {
                "status": "healthy",
                "message": "Redis connected"
            }
        else:
            health_status["checks"]["redis"] = {
                "status": "unhealthy",
                "message": "Redis client not initialized"
            }
            health_status["status"] = "unhealthy"
    except Exception as e:
        health_status["checks"]["redis"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "unhealthy"
    
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
            
            if vacancy_health.get("status") != "healthy":
                health_status["status"] = "degraded"
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
    
    # Проверка NLP инструментов
    try:
        patterns_loader = request.app.state.patterns_loader
        patterns_count = len(patterns_loader.get_all_patterns())
        
        health_status["checks"]["nlp_tools"] = {
            "status": "healthy",
            "message": f"NLP tools initialized with {patterns_count} patterns",
            "patterns_loaded": patterns_count
        }
    except Exception as e:
        health_status["checks"]["nlp_tools"] = {
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
    
    # Проверка кэша
    try:
        cache_stats = await cache_manager.get_cache_stats()
        health_status["cache"] = cache_stats
    except Exception as e:
        health_status["cache"] = {"error": str(e)}
    
    return health_status