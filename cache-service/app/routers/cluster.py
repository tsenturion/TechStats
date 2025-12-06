# C:\Users\user\Desktop\TechStats\cache-service\app\routers\cluster.py
import asyncio
import time
from typing import Dict, List, Any
from fastapi import APIRouter, HTTPException, Body, Query, Depends, Request
import structlog

from config import settings

router = APIRouter()
logger = structlog.get_logger()


async def get_cluster_manager(request: Request):
    """Dependency для получения менеджера кластера"""
    if hasattr(request.app.state, 'cluster_manager'):
        return request.app.state.cluster_manager
    return None


@router.get("/info")
async def get_cluster_info(
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Получение информации о кластере
    """
    if not cluster_manager:
        raise HTTPException(
            status_code=400,
            detail="Clustering is not enabled"
        )
    
    try:
        info = await cluster_manager.get_cluster_info()
        return info
    except Exception as e:
        logger.error("Get cluster info error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get cluster info: {str(e)}")


@router.post("/nodes/join")
async def join_cluster(
    request: Request,
    join_request: Dict[str, Any] = Body(...),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Присоединение новой ноды к кластеру
    """
    if not cluster_manager:
        raise HTTPException(
            status_code=400,
            detail="Clustering is not enabled"
        )
    
    node_url = join_request.get("url")
    node_id = join_request.get("id")
    
    if not node_url or not node_id:
        raise HTTPException(
            status_code=400,
            detail="URL and ID are required"
        )
    
    # Проверяем что нода еще не в кластере
    if node_id in cluster_manager.nodes:
        raise HTTPException(
            status_code=400,
            detail=f"Node {node_id} already in cluster"
        )
    
    try:
        # Добавляем ноду
        cluster_manager.nodes[node_id] = ClusterNode(
            id=node_id,
            url=node_url,
            status="joining",
            last_seen=time.time(),
            load=0.0,
            version=join_request.get("version", "unknown"),
            metadata=join_request.get("metadata", {})
        )
        
        # Перестраиваем hash ring
        cluster_manager._build_hash_ring()
        
        logger.info("Node joined cluster", node_id=node_id, url=node_url)
        
        return {
            "joined": True,
            "node_id": node_id,
            "cluster_size": len(cluster_manager.nodes),
            "welcome_message": f"Node {node_id} joined cluster successfully"
        }
        
    except Exception as e:
        logger.error("Join cluster error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to join cluster: {str(e)}")


@router.post("/nodes/leave")
async def leave_cluster(
    request: Request,
    leave_request: Dict[str, Any] = Body(...),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Выход ноды из кластера
    """
    if not cluster_manager:
        raise HTTPException(
            status_code=400,
            detail="Clustering is not enabled"
        )
    
    node_id = leave_request.get("id", settings.node_id)
    
    if node_id not in cluster_manager.nodes:
        raise HTTPException(
            status_code=404,
            detail=f"Node {node_id} not found in cluster"
        )
    
    try:
        # Удаляем ноду
        del cluster_manager.nodes[node_id]
        
        # Перестраиваем hash ring
        cluster_manager._build_hash_ring()
        
        logger.warning("Node left cluster", node_id=node_id)
        
        return {
            "left": True,
            "node_id": node_id,
            "cluster_size": len(cluster_manager.nodes),
            "message": f"Node {node_id} left cluster"
        }
        
    except Exception as e:
        logger.error("Leave cluster error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to leave cluster: {str(e)}")


@router.get("/nodes/{node_id}/health")
async def get_node_health(
    node_id: str,
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Проверка здоровья конкретной ноды
    """
    if not cluster_manager:
        raise HTTPException(
            status_code=400,
            detail="Clustering is not enabled"
        )
    
    if node_id not in cluster_manager.nodes:
        raise HTTPException(
            status_code=404,
            detail=f"Node {node_id} not found"
        )
    
    try:
        node = cluster_manager.nodes[node_id]
        
        # Если это текущая нода, возвращаем локальную информацию
        if node_id == settings.node_id:
            import psutil
            process = psutil.Process()
            
            return {
                "node_id": node_id,
                "status": "online",
                "last_seen": time.time(),
                "load": await cluster_manager._calculate_load(),
                "system": {
                    "memory_mb": process.memory_info().rss / 1024 / 1024,
                    "cpu_percent": process.cpu_percent(),
                    "uptime": time.time() - process.create_time()
                },
                "local": True
            }
        
        # Для других нод делаем health check
        await cluster_manager._check_node_health(node)
        
        return {
            "node_id": node_id,
            "status": node.status,
            "last_seen": node.last_seen,
            "load": node.load,
            "version": node.version,
            "metadata": node.metadata,
            "local": False
        }
        
    except Exception as e:
        logger.error("Get node health error", node_id=node_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get node health: {str(e)}")


@router.post("/rebalance")
async def rebalance_cluster(
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Перебалансировка кластера
    """
    if not cluster_manager:
        raise HTTPException(
            status_code=400,
            detail="Clustering is not enabled"
        )
    
    try:
        # Перестраиваем hash ring
        cluster_manager._build_hash_ring()
        
        # Получаем информацию о распределении
        distribution = cluster_manager._get_distribution_info()
        
        logger.info("Cluster rebalanced", distribution=distribution)
        
        return {
            "rebalanced": True,
            "distribution": distribution,
            "total_nodes": len(cluster_manager.nodes),
            "online_nodes": len([n for n in cluster_manager.nodes.values() if n.status == "online"]),
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error("Rebalance cluster error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to rebalance cluster: {str(e)}")


@router.get("/distribution")
async def get_distribution(
    key: str = Query(None, description="Ключ для проверки распределения"),
    cluster_manager = Depends(get_cluster_manager)
):
    """
    Получение информации о распределении ключей
    """
    if not cluster_manager:
        raise HTTPException(
            status_code=400,
            detail="Clustering is not enabled"
        )
    
    try:
        if key:
            # Для конкретного ключа
            node_id = cluster_manager.get_node_for_key(key)
            node = cluster_manager.nodes.get(node_id) if node_id else None
            
            return {
                "key": key,
                "assigned_node": node_id,
                "node_info": {
                    "id": node.id if node else None,
                    "url": node.url if node else None,
                    "status": node.status if node else None
                },
                "key_hash": cluster_manager._hash_key(key),
                "hash_ring_size": len(cluster_manager.consistent_hash_ring)
            }
        else:
            # Общая информация о распределении
            distribution = cluster_manager._get_distribution_info()
            
            # Статистика
            total_virtual = len(cluster_manager.consistent_hash_ring)
            node_stats = []
            
            for node_id, stats in distribution.items():
                node = cluster_manager.nodes.get(node_id)
                node_stats.append({
                    "node_id": node_id,
                    "status": node.status if node else "unknown",
                    "virtual_nodes": stats["virtual_nodes"],
                    "percentage": stats["percentage"],
                    "url": node.url if node else None
                })
            
            return {
                "distribution": distribution,
                "node_stats": node_stats,
                "total_virtual_nodes": total_virtual,
                "virtual_nodes_per_physical": cluster_manager.virtual_nodes,
                "hash_ring_size": len(cluster_manager.consistent_hash_ring)
            }
        
    except Exception as e:
        logger.error("Get distribution error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get distribution: {str(e)}")