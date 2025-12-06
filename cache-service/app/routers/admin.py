# C:\Users\user\Desktop\TechStats\cache-service\app\routers\admin.py
import asyncio
import time
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Body, Query, Depends, Request
from fastapi.responses import JSONResponse
import structlog

from config import settings
from app.cache_manager import CacheManager
from app.cleanup_scheduler import CleanupScheduler

router = APIRouter()
logger = structlog.get_logger()


async def get_cache_manager(request: Request) -> CacheManager:
    """Dependency для получения менеджера кэша"""
    return request.app.state.cache_manager


async def get_cleanup_scheduler(request: Request) -> CleanupScheduler:
    """Dependency для получения планировщика очистки"""
    return request.app.state.cleanup_scheduler


@router.get("/stats")
async def get_detailed_stats(
    cache_manager: CacheManager = Depends(get_cache_manager),
    cleanup_scheduler: CleanupScheduler = Depends(get_cleanup_scheduler)
):
    """
    Получение детальной статистики кэша
    """
    try:
        cache_stats = await cache_manager.get_stats()
        cleanup_stats = await cleanup_scheduler.get_stats()
        
        # Системная информация
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        
        system_info = {
            "memory_usage_mb": memory_info.rss / 1024 / 1024,
            "cpu_percent": process.cpu_percent(),
            "threads": process.num_threads(),
            "connections": process.num_connections() if hasattr(process, 'num_connections') else 0,
            "uptime": time.time() - process.create_time()
        }
        
        return {
            "cache": cache_stats,
            "cleanup": cleanup_stats,
            "system": system_info,
            "settings": {
                "backend": settings.cache_backend.value,
                "strategy": settings.cache_strategy.value,
                "default_ttl": settings.default_ttl_seconds,
                "max_size_mb": settings.max_cache_size_mb,
                "cleanup_interval": settings.cleanup_interval_seconds
            },
            "timestamp": time.time(),
            "node_id": settings.node_id
        }
        
    except Exception as e:
        logger.error("Get detailed stats error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@router.get("/stats/history")
async def get_stats_history(
    hours: int = Query(24, description="История за последние N часов"),
    interval: int = Query(1, description="Интервал в часах")
):
    """
    Получение исторической статистики
    """
    # В реальном приложении здесь был бы запрос к БД с метриками
    # Для примера возвращаем сгенерированные данные
    
    try:
        now = time.time()
        history = []
        
        for i in range(hours, 0, -interval):
            timestamp = now - (i * 3600)
            
            # Генерация данных
            history.append({
                "timestamp": timestamp,
                "time": time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp)),
                "hit_rate": 70 + (i % 30),  # Примерные данные
                "total_items": 10000 + (i * 100),
                "operations": {
                    "get": 5000 + (i * 50),
                    "set": 1000 + (i * 20),
                    "delete": 100 + (i * 5)
                },
                "memory_usage_mb": 200 + (i % 50)
            })
        
        return {
            "hours": hours,
            "interval_hours": interval,
            "data_points": len(history),
            "history": history,
            "node_id": settings.node_id
        }
        
    except Exception as e:
        logger.error("Get stats history error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")


@router.post("/flush")
async def flush_cache(
    request: Request,
    flush_request: Dict[str, Any] = Body(...),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """
    Полная очистка кэша
    """
    # Проверка безопасности (в production нужна аутентификация)
    confirm = flush_request.get("confirm", False)
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Confirmation required. Set confirm=true"
        )
    
    try:
        cleared = await cache_manager.clear("*")
        
        logger.warning("Cache flushed", cleared=cleared, initiator=request.client.host)
        
        return {
            "flushed": True,
            "keys_cleared": cleared,
            "node": settings.node_id,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Flush cache error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to flush cache: {str(e)}")


@router.post("/cleanup/trigger")
async def trigger_cleanup(
    cleanup_scheduler: CleanupScheduler = Depends(get_cleanup_scheduler)
):
    """
    Ручной запуск очистки кэша
    """
    try:
        # Запускаем очистку
        await cleanup_scheduler._perform_cleanup()
        
        # Получаем обновленную статистику
        cleanup_stats = await cleanup_scheduler.get_stats()
        
        return {
            "triggered": True,
            "cleanup_stats": cleanup_stats,
            "node": settings.node_id,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Trigger cleanup error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to trigger cleanup: {str(e)}")


@router.get("/monitor/keys")
async def monitor_keys(
    cache_manager: CacheManager = Depends(get_cache_manager),
    limit: int = Query(100, description="Максимальное количество ключей"),
    offset: int = Query(0, description="Смещение"),
    sort_by: str = Query("key", description="Сортировка (key, size, age)"),
    order: str = Query("asc", description="Порядок (asc, desc)")
):
    """
    Мониторинг ключей в кэше
    """
    try:
        # Получаем все ключи
        all_keys = await cache_manager.keys("*")
        
        if not all_keys:
            return {
                "keys": [],
                "total": 0,
                "limit": limit,
                "offset": offset
            }
        
        # Собираем информацию о ключах
        keys_info = []
        for key in all_keys:
            value = await cache_manager.get(key)
            if value is not None:
                # Оцениваем размер (очень приблизительно)
                try:
                    size = len(str(value).encode('utf-8'))
                except:
                    size = 0
                
                keys_info.append({
                    "key": key,
                    "size_bytes": size,
                    "has_value": True
                })
            else:
                keys_info.append({
                    "key": key,
                    "size_bytes": 0,
                    "has_value": False
                })
        
        # Сортировка
        reverse = order.lower() == "desc"
        
        if sort_by == "size":
            keys_info.sort(key=lambda x: x["size_bytes"], reverse=reverse)
        elif sort_by == "key":
            keys_info.sort(key=lambda x: x["key"], reverse=reverse)
        # Для age нужна дополнительная информация о времени создания
        
        # Пагинация
        total = len(keys_info)
        start = offset
        end = offset + limit
        paginated_keys = keys_info[start:end]
        
        # Суммарная статистика
        total_size = sum(k["size_bytes"] for k in keys_info)
        avg_size = total_size / total if total > 0 else 0
        
        return {
            "keys": paginated_keys,
            "total": total,
            "total_size_bytes": total_size,
            "avg_size_bytes": avg_size,
            "limit": limit,
            "offset": offset,
            "has_more": end < total,
            "node": settings.node_id
        }
        
    except Exception as e:
        logger.error("Monitor keys error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to monitor keys: {str(e)}")


@router.post("/config/update")
async def update_config(
    request: Request,
    config_update: Dict[str, Any] = Body(...)
):
    """
    Обновление конфигурации (динамическое)
    """
    # Внимание: в production нужна серьезная аутентификация и авторизация!
    
    try:
        updated_settings = []
        
        # Проверяем и обновляем настройки
        # В реальном приложении здесь была бы более сложная логика
        
        for key, value in config_update.items():
            # Проверяем что настройка существует и может быть обновлена
            if hasattr(settings, key):
                # Типа проверка (в реальном приложении нужна валидация)
                current_value = getattr(settings, key)
                logger.info(
                    "Config update attempt",
                    key=key,
                    current=current_value,
                    new=value
                )
                
                # Здесь можно добавить логику обновления
                updated_settings.append({
                    "key": key,
                    "previous": current_value,
                    "new": value,
                    "updated": False  # В демо не обновляем
                })
            else:
                logger.warning("Unknown config key", key=key)
        
        return {
            "updated": updated_settings,
            "message": "Config changes logged (not applied in demo)",
            "node": settings.node_id,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Update config error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to update config: {str(e)}")


@router.get("/export")
async def export_cache(
    cache_manager: CacheManager = Depends(get_cache_manager),
    format: str = Query("json", description="Формат экспорта (json, msgpack)"),
    limit: int = Query(1000, description="Максимальное количество элементов")
):
    """
    Экспорт данных из кэша
    """
    try:
        # Получаем ключи
        all_keys = await cache_manager.keys("*")
        
        if not all_keys:
            return {
                "exported": 0,
                "format": format,
                "data": {},
                "message": "Cache is empty"
            }
        
        # Ограничиваем количество
        if len(all_keys) > limit:
            all_keys = all_keys[:limit]
            logger.warning("Too many keys for export, limiting", limit=limit)
        
        # Получаем значения
        values = await cache_manager.mget(all_keys)
        
        # Фильтруем None значения
        export_data = {k: v for k, v in values.items() if v is not None}
        
        # Подготавливаем ответ в зависимости от формата
        if format == "msgpack":
            import msgpack
            packed = msgpack.packb(export_data, use_bin_type=True)
            
            from fastapi.responses import Response
            return Response(
                content=packed,
                media_type="application/msgpack",
                headers={
                    "Content-Disposition": f"attachment; filename=cache_export_{int(time.time())}.msgpack"
                }
            )
        else:
            # JSON по умолчанию
            return {
                "exported": len(export_data),
                "format": format,
                "total_keys": len(all_keys),
                "data": export_data,
                "node": settings.node_id,
                "timestamp": time.time()
            }
        
    except Exception as e:
        logger.error("Export cache error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to export cache: {str(e)}")