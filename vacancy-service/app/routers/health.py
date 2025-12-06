# C:\Users\user\Desktop\TechStats\vacancy-service\app\routers\health.py
import asyncio
from datetime import datetime
from typing import Dict, Any
import httpx
import redis.asyncio as redis
from fastapi import APIRouter, HTTPException
import structlog

from config import settings
from app.cache import cache_manager

router = APIRouter()
logger = structlog.get_logger()


@router.get("/health")
async def health_check():
    """
    Проверка здоровья сервиса
    """
    health_status = {
        "service": "vacancy-service",
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
    except Exception as e:
        health_status["checks"]["redis"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "unhealthy"
    
    # Проверка HH API
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{settings.hh_api_base_url}/")
            if response.status_code == 200:
                health_status["checks"]["hh_api"] = {
                    "status": "healthy",
                    "message": "HH API accessible",
                    "response_time": response.elapsed.total_seconds()
                }
            else:
                health_status["checks"]["hh_api"] = {
                    "status": "degraded",
                    "message": f"HH API returned {response.status_code}",
                    "response_time": response.elapsed.total_seconds()
                }
                health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["hh_api"] = {
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


@router.get("/health/detailed")
async def detailed_health_check():
    """
    Детальная проверка здоровья сервиса
    """
    health_status = await health_check()
    
    # Дополнительные проверки
    additional_checks = {}
    
    # Проверка скорости Redis
    try:
        if cache_manager.redis_client:
            start_time = asyncio.get_event_loop().time()
            for _ in range(100):
                await cache_manager.redis_client.ping()
            end_time = asyncio.get_event_loop().time()
            
            additional_checks["redis_speed"] = {
                "status": "healthy",
                "operations_per_second": 100 / (end_time - start_time),
                "latency_ms": (end_time - start_time) * 1000 / 100
            }
    except Exception as e:
        additional_checks["redis_speed"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # Проверка доступности HH API endpoints
    endpoints_to_check = [
        "/vacancies",
        "/areas",
        "/industries",
        "/professional_roles"
    ]
    
    endpoint_health = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for endpoint in endpoints_to_check:
            try:
                response = await client.get(f"{settings.hh_api_base_url}{endpoint}")
                endpoint_health[endpoint] = {
                    "status": "healthy" if response.status_code == 200 else "unhealthy",
                    "status_code": response.status_code,
                    "response_time": response.elapsed.total_seconds()
                }
            except Exception as e:
                endpoint_health[endpoint] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
    
    additional_checks["hh_endpoints"] = endpoint_health
    health_status["detailed_checks"] = additional_checks
    
    return health_status


@router.get("/health/ready")
async def readiness_probe():
    """
    Проверка готовности сервиса к работе
    """
    health = await health_check()
    
    if health["status"] in ["healthy", "degraded"]:
        return {"status": "ready", "service": "vacancy-service"}
    else:
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/health/live")
async def liveness_probe():
    """
    Проверка активности сервиса
    """
    return {"status": "alive", "service": "vacancy-service", "timestamp": datetime.now().isoformat()}