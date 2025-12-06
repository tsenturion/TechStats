# C:\Users\user\Desktop\TechStats\cache-service\app\routers\cache.py
import asyncio
import time
from typing import Dict, List, Any, Optional, Union
from fastapi import APIRouter, HTTPException, Body, Query, Depends, Request
from fastapi.responses import JSONResponse
import structlog

from config import settings
from app.cache_manager import CacheManager

router = APIRouter()
logger = structlog.get_logger()


async def get_cache_manager(request: Request) -> CacheManager:
    """Dependency для получения менеджера кэша"""
    return request.app.state.cache_manager


async def get_cluster_manager(request: Request):
    """Dependency для получения менеджера кластера"""
    if hasattr(request.app.state, 'cluster_manager'):
        return request.app.state.cluster_manager
    return None


@router.get("/cache/{key}")
async def get_cache(
    request: Request,
    key: str,
    cache_manager: CacheManager = Depends(get_cache_manager),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Получение значения из кэша по ключу
    """
    # Проверяем нужно ли маршрутизировать запрос
    if cluster_manager:
        target_node = await cluster_manager.route_request(key)
        if target_node and target_node != f"http://{settings.node_id}:{settings.port}":
            # Проксируем запрос на нужную ноду
            logger.debug("Routing request to another node", key=key, target=target_node)
            return JSONResponse(
                status_code=307,
                headers={"Location": f"{target_node}/api/v1/cache/{key}"}
            )
    
    try:
        value = await cache_manager.get(key)
        
        if value is None:
            raise HTTPException(status_code=404, detail="Key not found")
        
        return {
            "key": key,
            "value": value,
            "found": True,
            "node": settings.node_id,
            "timestamp": time.time()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Get cache error", key=key, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get value: {str(e)}")


@router.put("/cache/{key}")
async def set_cache(
    request: Request,
    key: str,
    cache_data: Dict[str, Any] = Body(...),
    cache_manager: CacheManager = Depends(get_cache_manager),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Сохранение значения в кэш
    """
    if "value" not in cache_data:
        raise HTTPException(status_code=400, detail="Value field is required")
    
    value = cache_data["value"]
    ttl = cache_data.get("ttl")
    tags = cache_data.get("tags", [])
    
    # Проверяем нужно ли маршрутизировать запрос
    primary_node = None
    if cluster_manager:
        primary_node = await cluster_manager.route_request(key)
    
    try:
        if primary_node and primary_node != f"http://{settings.node_id}:{settings.port}":
            # Основная нода - другая, сохраняем там
            logger.debug("Primary node is different", key=key, primary=primary_node)
            
            # Сохраняем локально и реплицируем
            local_success = await cache_manager.set(key, value, ttl, tags)
            
            if local_success and settings.enable_clustering:
                # Реплицируем на другие ноды
                await cluster_manager.replicate_data(key, value, ttl)
            
            return {
                "key": key,
                "success": local_success,
                "primary_node": primary_node.split("://")[1].split(":")[0],
                "local_node": settings.node_id,
                "replicated": settings.enable_clustering,
                "timestamp": time.time()
            }
        else:
            # Мы - основная нода для этого ключа
            success = await cache_manager.set(key, value, ttl, tags)
            
            if success and cluster_manager:
                # Реплицируем на другие ноды
                await cluster_manager.replicate_data(key, value, ttl)
            
            return {
                "key": key,
                "success": success,
                "primary_node": settings.node_id,
                "replicated": settings.enable_clustering and cluster_manager is not None,
                "timestamp": time.time()
            }
            
    except Exception as e:
        logger.error("Set cache error", key=key, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to set value: {str(e)}")


@router.post("/cache/replicate")
async def replicate_cache(
    request: Request,
    replicate_data: Dict[str, Any] = Body(...),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """
    Внутренний endpoint для репликации данных (только для кластера)
    """
    # Проверяем секретный ключ для безопасности
    secret = request.headers.get("X-Replication-Secret")
    expected_secret = f"replicate-{settings.node_id}-{int(time.time() / 3600)}"  # Меняется каждый час
    
    # Простая проверка (в production нужно что-то более безопасное)
    if not secret or not secret.startswith("replicate-"):
        raise HTTPException(status_code=403, detail="Replication not allowed")
    
    key = replicate_data.get("key")
    value = replicate_data.get("value")
    ttl = replicate_data.get("ttl")
    
    if not key or value is None:
        raise HTTPException(status_code=400, detail="Key and value are required")
    
    try:
        success = await cache_manager.set(key, value, ttl)
        
        return {
            "key": key,
            "success": success,
            "node": settings.node_id,
            "replicated": True,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Replicate cache error", key=key, error=str(e))
        raise HTTPException(status_code=500, detail=f"Replication failed: {str(e)}")


@router.delete("/cache/{key}")
async def delete_cache(
    request: Request,
    key: str,
    cache_manager: CacheManager = Depends(get_cache_manager),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Удаление значения из кэша
    """
    # Проверяем нужно ли маршрутизировать запрос
    if cluster_manager:
        target_node = await cluster_manager.route_request(key)
        if target_node and target_node != f"http://{settings.node_id}:{settings.port}":
            # Проксируем запрос на нужную ноду
            return JSONResponse(
                status_code=307,
                headers={"Location": f"{target_node}/api/v1/cache/{key}"}
            )
    
    try:
        success = await cache_manager.delete(key)
        
        if not success:
            raise HTTPException(status_code=404, detail="Key not found")
        
        return {
            "key": key,
            "deleted": True,
            "node": settings.node_id,
            "timestamp": time.time()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Delete cache error", key=key, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to delete value: {str(e)}")


@router.post("/cache/mget")
async def multi_get_cache(
    request: Request,
    keys_request: Dict[str, Any] = Body(...),
    cache_manager: CacheManager = Depends(get_cache_manager),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Пакетное получение значений из кэша
    """
    keys = keys_request.get("keys", [])
    
    if not keys or not isinstance(keys, list):
        raise HTTPException(status_code=400, detail="Keys must be a non-empty list")
    
    # Ограничиваем количество ключей
    if len(keys) > 1000:
        keys = keys[:1000]
        logger.warning("Too many keys, limiting to 1000")
    
    try:
        # Если кластер включен, нужно распределить запросы
        if cluster_manager:
            # Группируем ключи по нодам
            keys_by_node: Dict[str, List[str]] = {}
            local_keys = []
            
            for key in keys:
                target_node = await cluster_manager.route_request(key)
                if target_node and target_node != f"http://{settings.node_id}:{settings.port}":
                    node_id = target_node.split("://")[1].split(":")[0]
                    if node_id not in keys_by_node:
                        keys_by_node[node_id] = []
                    keys_by_node[node_id].append(key)
                else:
                    local_keys.append(key)
            
            # Собираем результаты
            all_results = {}
            
            # Локальные ключи
            if local_keys:
                local_results = await cache_manager.mget(local_keys)
                all_results.update(local_results)
            
            # Ключи с других нод (параллельно)
            tasks = []
            for node_id, node_keys in keys_by_node.items():
                node = cluster_manager.nodes.get(node_id)
                if node and node.status == "online":
                    task = _fetch_from_node(node, node_keys)
                    tasks.append(task)
            
            if tasks:
                node_results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in node_results:
                    if isinstance(result, dict):
                        all_results.update(result)
            
            return {
                "keys": keys,
                "results": all_results,
                "total_requested": len(keys),
                "total_found": sum(1 for v in all_results.values() if v is not None),
                "nodes_involved": len(keys_by_node) + (1 if local_keys else 0)
            }
        else:
            # Без кластера - просто получаем все локально
            results = await cache_manager.mget(keys)
            
            return {
                "keys": keys,
                "results": results,
                "total_requested": len(keys),
                "total_found": sum(1 for v in results.values() if v is not None),
                "node": settings.node_id
            }
            
    except Exception as e:
        logger.error("Multi get cache error", keys=keys, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get values: {str(e)}")


async def _fetch_from_node(node, keys: List[str]) -> Dict[str, Any]:
    """Получение значений с другой ноды"""
    try:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{node.url}/api/v1/cache/mget",
                json={"keys": keys}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("results", {})
                else:
                    logger.warning("Failed to fetch from node", node_id=node.id, status=response.status)
                    return {key: None for key in keys}
    
    except Exception as e:
        logger.warning("Fetch from node error", node_id=node.id, error=str(e))
        return {key: None for key in keys}


@router.post("/cache/mset")
async def multi_set_cache(
    request: Request,
    items_request: Dict[str, Any] = Body(...),
    cache_manager: CacheManager = Depends(get_cache_manager),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Пакетное сохранение значений в кэш
    """
    items = items_request.get("items", {})
    ttl = items_request.get("ttl")
    tags = items_request.get("tags", {})
    
    if not items or not isinstance(items, dict):
        raise HTTPException(status_code=400, detail="Items must be a non-empty dict")
    
    # Ограничиваем количество элементов
    if len(items) > 1000:
        items = dict(list(items.items())[:1000])
        logger.warning("Too many items, limiting to 1000")
    
    try:
        # Если кластер включен, нужно распределить запись
        if cluster_manager:
            # Группируем элементы по нодам
            items_by_node: Dict[str, Dict[str, Any]] = {}
            local_items = {}
            tags_by_node: Dict[str, Dict[str, List[str]]] = {}
            
            for key, value in items.items():
                target_node = await cluster_manager.route_request(key)
                if target_node and target_node != f"http://{settings.node_id}:{settings.port}":
                    node_id = target_node.split("://")[1].split(":")[0]
                    if node_id not in items_by_node:
                        items_by_node[node_id] = {}
                        tags_by_node[node_id] = {}
                    
                    items_by_node[node_id][key] = value
                    if key in tags:
                        tags_by_node[node_id][key] = tags[key]
                else:
                    local_items[key] = value
            
            # Сохраняем локальные элементы
            success = True
            if local_items:
                local_tags = {k: v for k, v in tags.items() if k in local_items}
                success = await cache_manager.mset(local_items, ttl, local_tags)
            
            # Сохраняем на других нодах (параллельно)
            tasks = []
            for node_id, node_items in items_by_node.items():
                node = cluster_manager.nodes.get(node_id)
                if node and node.status == "online":
                    node_tags = tags_by_node.get(node_id, {})
                    task = _store_to_node(node, node_items, ttl, node_tags)
                    tasks.append(task)
            
            if tasks:
                node_results = await asyncio.gather(*tasks, return_exceptions=True)
                node_success = all(isinstance(r, dict) and r.get("success") for r in node_results if not isinstance(r, Exception))
                success = success and node_success
            
            return {
                "success": success,
                "total_items": len(items),
                "nodes_involved": len(items_by_node) + (1 if local_items else 0),
                "cluster_mode": True
            }
        else:
            # Без кластера - просто сохраняем все локально
            success = await cache_manager.mset(items, ttl, tags)
            
            return {
                "success": success,
                "total_items": len(items),
                "node": settings.node_id,
                "cluster_mode": False
            }
            
    except Exception as e:
        logger.error("Multi set cache error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to set values: {str(e)}")


async def _store_to_node(node, items: Dict[str, Any], ttl: Optional[int], tags: Dict[str, List[str]]) -> Dict[str, Any]:
    """Сохранение значений на другой ноде"""
    try:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=10)
        
        payload = {"items": items}
        if ttl:
            payload["ttl"] = ttl
        if tags:
            payload["tags"] = tags
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{node.url}/api/v1/cache/mset",
                json=payload
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning("Failed to store to node", node_id=node.id, status=response.status)
                    return {"success": False}
    
    except Exception as e:
        logger.warning("Store to node error", node_id=node.id, error=str(e))
        return {"success": False}


@router.get("/cache/keys")
async def get_cache_keys(
    request: Request,
    pattern: str = Query("*", description="Паттерн для поиска ключей"),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """
    Получение списка ключей по паттерну
    """
    try:
        keys = await cache_manager.keys(pattern)
        
        return {
            "pattern": pattern,
            "keys": keys,
            "count": len(keys),
            "node": settings.node_id
        }
        
    except Exception as e:
        logger.error("Get keys error", pattern=pattern, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get keys: {str(e)}")


@router.delete("/cache/clear")
async def clear_cache(
    request: Request,
    pattern: str = Query("*", description="Паттерн для очистки"),
    cache_manager: CacheManager = Depends(get_cache_manager),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Очистка кэша по паттерну
    """
    try:
        if cluster_manager:
            # В кластере нужно очистить на всех нодах
            tasks = []
            
            # Локальная очистка
            local_task = cache_manager.clear(pattern)
            tasks.append(local_task)
            
            # Очистка на других нодах
            for node_id, node in cluster_manager.nodes.items():
                if node_id != settings.node_id and node.status == "online":
                    task = _clear_on_node(node, pattern)
                    tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Суммируем результаты
            total_cleared = 0
            for result in results:
                if isinstance(result, int):
                    total_cleared += result
            
            return {
                "pattern": pattern,
                "cleared": total_cleared,
                "nodes_cleared": len([r for r in results if isinstance(r, int) and r > 0]),
                "cluster_mode": True
            }
        else:
            # Без кластера
            cleared = await cache_manager.clear(pattern)
            
            return {
                "pattern": pattern,
                "cleared": cleared,
                "node": settings.node_id,
                "cluster_mode": False
            }
            
    except Exception as e:
        logger.error("Clear cache error", pattern=pattern, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")


async def _clear_on_node(node, pattern: str) -> int:
    """Очистка кэша на другой ноде"""
    try:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.delete(
                f"{node.url}/api/v1/cache/clear",
                params={"pattern": pattern}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("cleared", 0)
                else:
                    logger.warning("Failed to clear on node", node_id=node.id, status=response.status)
                    return 0
    
    except Exception as e:
        logger.warning("Clear on node error", node_id=node.id, error=str(e))
        return 0


@router.post("/cache/invalidate/tags")
async def invalidate_by_tags(
    request: Request,
    tags_request: Dict[str, Any] = Body(...),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """
    Инвалидация кэша по тегам
    """
    tags = tags_request.get("tags", [])
    
    if not tags or not isinstance(tags, list):
        raise HTTPException(status_code=400, detail="Tags must be a non-empty list")
    
    try:
        cleared = await cache_manager.invalidate_by_tags(tags)
        
        return {
            "tags": tags,
            "cleared": cleared,
            "node": settings.node_id
        }
        
    except Exception as e:
        logger.error("Invalidate by tags error", tags=tags, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to invalidate cache: {str(e)}")