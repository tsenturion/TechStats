# C:\Users\user\Desktop\TechStats\cache-service\app\routers\health.py
import asyncio
from datetime import datetime
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Request
import structlog

from config import settings
from app.cache_manager import CacheManager

router = APIRouter()
logger = structlog.get_logger()


async def get_cache_manager(request: Request) -> CacheManager:
    """Dependency для получения менеджера кэша"""
    return request.app.state.cache_manager


@router.get("/health")
async def health_check(
    request: Request,
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """
    Проверка здоровья сервиса
    """
    health_status = {
        "service": "cache-service",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": settings.version,
        "environment": settings.environment,
        "node_id": settings.node_id,
        "checks": {}
    }
    
    # Проверка бэкенда кэша
    try:
        stats = await cache_manager.get_stats()
        
        if "error" in stats:
            health_status["checks"]["cache_backend"] = {
                "status": "unhealthy",
                "message": stats["error"]
            }
            health_status["status"] = "unhealthy"
        else:
            health_status["checks"]["cache_backend"] = {
                "status": "healthy",
                "message": f"{settings.cache_backend.value} backend operational",
                "stats": stats.get("backend", {})
            }
    except Exception as e:
        health_status["checks"]["cache_backend"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "unhealthy"
    
    # Проверка планировщика очистки
    try:
        if hasattr(request.app.state, 'cleanup_scheduler'):
            scheduler = request.app.state.cleanup_scheduler
            scheduler_stats = await scheduler.get_stats()
            
            health_status["checks"]["cleanup_scheduler"] = {
                "status": "healthy",
                "message": "Cleanup scheduler running",
                "stats": scheduler_stats
            }
        else:
            health_status["checks"]["cleanup_scheduler"] = {
                "status": "healthy",
                "message": "Cleanup scheduler not initialized (might be disabled)"
            }
    except Exception as e:
        health_status["checks"]["cleanup_scheduler"] = {
            "status": "unhealthy",
            "message": str(e)
        }
    
    # Проверка кластера (если включен)
    try:
        if settings.enable_clustering and hasattr(request.app.state, 'cluster_manager'):
            cluster_manager = request.app.state.cluster_manager
            cluster_info = await cluster_manager.get_cluster_info()
            
            online_nodes = cluster_info["nodes"]["online"]
            total_nodes = cluster_info["nodes"]["total"]
            
            if online_nodes == 0:
                health_status["checks"]["cluster"] = {
                    "status": "unhealthy",
                    "message": "No online nodes in cluster"
                }
                health_status["status"] = "degraded"
            elif online_nodes < total_nodes:
                health_status["checks"]["cluster"] = {
                    "status": "degraded",
                    "message": f"{online_nodes}/{total_nodes} nodes online",
                    "cluster_info": cluster_info
                }
                health_status["status"] = "degraded"
            else:
                health_status["checks"]["cluster"] = {
                    "status": "healthy",
                    "message": f"Cluster with {online_nodes} nodes operational",
                    "cluster_info": cluster_info
                }
        elif settings.enable_clustering:
            health_status["checks"]["cluster"] = {
                "status": "unhealthy",
                "message": "Cluster enabled but manager not initialized"
            }
            health_status["status"] = "degraded"
        else:
            health_status["checks"]["cluster"] = {
                "status": "healthy",
                "message": "Clustering disabled, single node mode"
            }
    except Exception as e:
        health_status["checks"]["cluster"] = {
            "status": "unhealthy",
            "message": str(e)
        }
        health_status["status"] = "degraded"
    
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
    
    # Проверка нагрузки
    memory_percent = psutil.virtual_memory().percent
    if memory_percent > 90:
        health_status["status"] = "degraded"
        health_status["checks"]["memory"] = {
            "status": "warning",
            "message": f"High memory usage: {memory_percent}%"
        }
    
    return health_status


@router.get("/health/ready")
async def readiness_probe(
    request: Request,
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """
    Проверка готовности сервиса к работе
    """
    try:
        # Простая проверка что кэш работает
        test_key = f"readiness_check:{settings.node_id}"
        test_value = {"status": "ready", "timestamp": time.time()}
        
        await cache_manager.set(test_key, test_value, ttl=10)
        retrieved = await cache_manager.get(test_key)
        
        if retrieved and retrieved.get("status") == "ready":
            return {"status": "ready", "service": "cache-service", "node_id": settings.node_id}
        else:
            raise HTTPException(status_code=503, detail="Cache not ready")
            
    except Exception as e:
        logger.error("Readiness check failed", error=str(e))
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/health/live")
async def liveness_probe():
    """
    Проверка активности сервиса
    """
    return {
        "status": "alive",
        "service": "cache-service",
        "node_id": settings.node_id,
        "timestamp": datetime.now().isoformat()
    }