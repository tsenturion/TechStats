# C:\Users\user\Desktop\TechStats\cache-service\app\cluster_manager.py
import asyncio
import time
import hashlib
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, asdict
import aiohttp
import structlog

from config import settings

logger = structlog.get_logger()


@dataclass
class ClusterNode:
    """Информация о ноде кластера"""
    id: str
    url: str
    status: str  # online, offline, joining, leaving
    last_seen: float
    load: float  # 0-100
    version: str
    metadata: Dict[str, Any]


class ClusterManager:
    """Менеджер кластера для распределенного кэша"""
    
    def __init__(self):
        self.nodes: Dict[str, ClusterNode] = {}
        self.self_node: Optional[ClusterNode] = None
        self.health_check_task: Optional[asyncio.Task] = None
        self.running = False
        self.consistent_hash_ring: Dict[int, str] = {}
        self.virtual_nodes = 100
    
    async def initialize(self):
        """Инициализация кластера"""
        if not settings.enable_clustering:
            logger.info("Clustering disabled, running in single-node mode")
            return
        
        # Создаем информацию о текущей ноде
        self.self_node = ClusterNode(
            id=settings.node_id,
            url=f"http://{settings.node_id}:{settings.port}",
            status="online",
            last_seen=time.time(),
            load=0.0,
            version=settings.version,
            metadata={
                "cache_backend": settings.cache_backend.value,
                "cache_strategy": settings.cache_strategy.value,
                "started_at": time.time()
            }
        )
        
        # Добавляем себя в список нод
        self.nodes[settings.node_id] = self.self_node
        
        # Добавляем другие ноды из конфигурации
        for node_url in settings.cluster_nodes:
            node_id = self._extract_node_id(node_url)
            if node_id != settings.node_id:  # Не добавляем себя
                self.nodes[node_id] = ClusterNode(
                    id=node_id,
                    url=node_url,
                    status="unknown",
                    last_seen=0,
                    load=0.0,
                    version="unknown",
                    metadata={}
                )
        
        # Строим consistent hash ring
        self._build_hash_ring()
        
        # Запускаем проверку здоровья
        self.running = True
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info(
            "Cluster initialized",
            node_id=settings.node_id,
            total_nodes=len(self.nodes),
            hash_ring_size=len(self.consistent_hash_ring)
        )
    
    def _extract_node_id(self, url: str) -> str:
        """Извлечение ID ноды из URL"""
        # Пример: http://cache-node-1:8003 -> cache-node-1
        if "://" in url:
            url = url.split("://")[1]
        
        if ":" in url:
            url = url.split(":")[0]
        
        return url
    
    def _build_hash_ring(self):
        """Построение consistent hash ring"""
        self.consistent_hash_ring.clear()
        
        for node_id, node in self.nodes.items():
            if node.status == "online":
                # Добавляем виртуальные ноды для лучшего распределения
                for i in range(self.virtual_nodes):
                    virtual_node_id = f"{node_id}#{i}"
                    hash_value = self._hash_key(virtual_node_id)
                    self.consistent_hash_ring[hash_value] = node_id
        
        # Сортируем ключи
        self.consistent_hash_ring = dict(sorted(self.consistent_hash_ring.items()))
        
        logger.debug("Hash ring built", size=len(self.consistent_hash_ring))
    
    def _hash_key(self, key: str) -> int:
        """Хэширование ключа"""
        # Используем MD5 для распределения
        hash_obj = hashlib.md5(key.encode())
        return int(hash_obj.hexdigest(), 16) % (2**32)
    
    def get_node_for_key(self, key: str) -> Optional[str]:
        """Получение ноды для ключа"""
        if not self.consistent_hash_ring:
            return None
        
        key_hash = self._hash_key(key)
        
        # Ищем первую ноду с хэшем >= key_hash
        for node_hash in sorted(self.consistent_hash_ring.keys()):
            if node_hash >= key_hash:
                return self.consistent_hash_ring[node_hash]
        
        # Если не нашли, берем первую ноду (кольцо)
        return self.consistent_hash_ring[min(self.consistent_hash_ring.keys())]
    
    async def _health_check_loop(self):
        """Цикл проверки здоровья нод"""
        while self.running:
            try:
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд
                await self._check_nodes_health()
                self._build_hash_ring()  # Перестраиваем ring после проверки
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Health check loop error", error=str(e))
                await asyncio.sleep(60)
    
    async def _check_nodes_health(self):
        """Проверка здоровья всех нод"""
        tasks = []
        
        for node_id, node in self.nodes.items():
            if node_id == settings.node_id:
                # Обновляем информацию о себе
                node.last_seen = time.time()
                node.load = await self._calculate_load()
                continue
            
            # Асинхронная проверка других нод
            task = self._check_node_health(node)
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _calculate_load(self) -> float:
        """Расчет нагрузки текущей ноды"""
        # Простая реализация - можно расширить
        import psutil
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_percent = psutil.virtual_memory().percent
        
        # Усредняем CPU и память
        return (cpu_percent + memory_percent) / 2
    
    async def _check_node_health(self, node: ClusterNode) -> None:
        """Проверка здоровья конкретной ноды"""
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{node.url}/api/v1/health") as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Обновляем информацию о ноде
                        node.status = "online"
                        node.last_seen = time.time()
                        node.version = data.get("version", "unknown")
                        node.metadata = data.get("metadata", {})
                        
                        logger.debug("Node is healthy", node_id=node.id)
                    else:
                        node.status = "offline"
                        logger.warning("Node health check failed", node_id=node.id, status=response.status)
        
        except Exception as e:
            node.status = "offline"
            logger.warning("Node health check error", node_id=node.id, error=str(e))
    
    async def get_cluster_info(self) -> Dict[str, Any]:
        """Получение информации о кластере"""
        online_nodes = [n for n in self.nodes.values() if n.status == "online"]
        offline_nodes = [n for n in self.nodes.values() if n.status == "offline"]
        
        return {
            "cluster_enabled": settings.enable_clustering,
            "self_node": asdict(self.self_node) if self.self_node else None,
            "nodes": {
                "total": len(self.nodes),
                "online": len(online_nodes),
                "offline": len(offline_nodes),
                "list": [asdict(n) for n in self.nodes.values()]
            },
            "hash_ring": {
                "size": len(self.consistent_hash_ring),
                "virtual_nodes_per_physical": self.virtual_nodes
            },
            "distribution": self._get_distribution_info()
        }
    
    def _get_distribution_info(self) -> Dict[str, Any]:
        """Получение информации о распределении"""
        if not self.consistent_hash_ring:
            return {"error": "Hash ring not built"}
        
        # Считаем сколько ключей приходится на каждую ноду
        distribution = {}
        for node_id in set(self.consistent_hash_ring.values()):
            count = list(self.consistent_hash_ring.values()).count(node_id)
            percentage = (count / len(self.consistent_hash_ring)) * 100
            distribution[node_id] = {
                "virtual_nodes": count,
                "percentage": round(percentage, 2)
            }
        
        return distribution
    
    async def route_request(self, key: str) -> Optional[str]:
        """Маршрутизация запроса на нужную ноду"""
        if not settings.enable_clustering:
            return None
        
        node_id = self.get_node_for_key(key)
        if node_id and node_id != settings.node_id:
            node = self.nodes.get(node_id)
            if node and node.status == "online":
                return node.url
        
        return None
    
    async def replicate_data(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Репликация данных на другие ноды"""
        if not settings.enable_clustering:
            return True
        
        # Находим ноды для репликации (соседние в hash ring)
        replica_nodes = self._get_replica_nodes(key, replication_factor=2)
        
        tasks = []
        for node_id in replica_nodes:
            if node_id != settings.node_id:
                node = self.nodes.get(node_id)
                if node and node.status == "online":
                    task = self._replicate_to_node(node, key, value, ttl)
                    tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            logger.debug("Replication completed", key=key, success=success_count, total=len(tasks))
            return success_count > 0
        
        return True
    
    def _get_replica_nodes(self, key: str, replication_factor: int = 2) -> List[str]:
        """Получение нод для репликации"""
        if not self.consistent_hash_ring:
            return []
        
        key_hash = self._hash_key(key)
        sorted_hashes = sorted(self.consistent_hash_ring.keys())
        
        # Находим позицию ключа в ring
        start_idx = 0
        for i, node_hash in enumerate(sorted_hashes):
            if node_hash >= key_hash:
                start_idx = i
                break
        
        # Берем следующие N нод для репликации
        replica_nodes = []
        for i in range(replication_factor):
            idx = (start_idx + i) % len(sorted_hashes)
            node_id = self.consistent_hash_ring[sorted_hashes[idx]]
            if node_id not in replica_nodes:
                replica_nodes.append(node_id)
        
        return replica_nodes
    
    async def _replicate_to_node(
        self,
        node: ClusterNode,
        key: str,
        value: Any,
        ttl: Optional[int]
    ) -> bool:
        """Репликация данных на конкретную ноду"""
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                payload = {"key": key, "value": value}
                if ttl:
                    payload["ttl"] = ttl
                
                async with session.post(
                    f"{node.url}/api/v1/cache/replicate",
                    json=payload
                ) as response:
                    return response.status == 200
        
        except Exception as e:
            logger.warning("Replication failed", node_id=node.id, key=key, error=str(e))
            return False
    
    async def shutdown(self):
        """Завершение работы кластера"""
        self.running = False
        
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Cluster manager shutdown")