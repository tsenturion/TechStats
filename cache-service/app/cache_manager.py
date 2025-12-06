# C:\Users\user\Desktop\TechStats\cache-service\app\cache_manager.py
import asyncio
import time
import json
import hashlib
import pickle
import zlib
from typing import Dict, Any, List, Optional, Union, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import redis.asyncio as redis
from redis.asyncio.cluster import RedisCluster
from pymongo import MongoClient
from pymongo.database import Database
import msgpack
import orjson
import structlog

from config import settings, CacheBackend, CacheStrategy

logger = structlog.get_logger()


class CacheItem:
    """Класс для представления элемента кэша"""
    
    def __init__(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        created_at: Optional[float] = None,
        accessed_at: Optional[float] = None,
        access_count: int = 0,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.key = key
        self.value = value
        self.ttl = ttl or settings.default_ttl_seconds
        self.created_at = created_at or time.time()
        self.accessed_at = accessed_at or self.created_at
        self.access_count = access_count
        self.tags = tags or []
        self.metadata = metadata or {}
    
    def is_expired(self) -> bool:
        """Проверка истек ли срок действия"""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь"""
        return {
            "key": self.key,
            "value": self.value,
            "ttl": self.ttl,
            "created_at": self.created_at,
            "accessed_at": self.accessed_at,
            "access_count": self.access_count,
            "tags": self.tags,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CacheItem":
        """Создание из словаря"""
        return cls(
            key=data["key"],
            value=data["value"],
            ttl=data.get("ttl"),
            created_at=data.get("created_at"),
            accessed_at=data.get("accessed_at"),
            access_count=data.get("access_count", 0),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {})
        )


class CacheBackendInterface(ABC):
    """Интерфейс для бэкендов кэша"""
    
    @abstractmethod
    async def initialize(self):
        """Инициализация бэкенда"""
        pass
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Получение значения по ключу"""
        pass
    
    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Сохранение значения"""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Удаление значения"""
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Проверка существования ключа"""
        pass
    
    @abstractmethod
    async def mget(self, keys: List[str]) -> Dict[str, Optional[Any]]:
        """Пакетное получение значений"""
        pass
    
    @abstractmethod
    async def mset(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        tags: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """Пакетное сохранение значений"""
        pass
    
    @abstractmethod
    async def keys(self, pattern: str = "*") -> List[str]:
        """Получение ключей по паттерну"""
        pass
    
    @abstractmethod
    async def clear(self, pattern: str = "*") -> int:
        """Очистка кэша по паттерну"""
        pass
    
    @abstractmethod
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики бэкенда"""
        pass
    
    @abstractmethod
    async def shutdown(self):
        """Завершение работы бэкенда"""
        pass


class RedisBackend(CacheBackendInterface):
    """Redis бэкенд для кэша"""
    
    def __init__(self):
        self.client: Optional[Union[redis.Redis, RedisCluster]] = None
        self.initialized = False
    
    async def initialize(self):
        """Инициализация Redis клиента"""
        try:
            if settings.cache_backend == CacheBackend.REDIS_CLUSTER:
                # Redis Cluster
                startup_nodes = [
                    {"host": node.split("://")[1].split(":")[0], 
                     "port": int(node.split("://")[1].split(":")[1])}
                    for node in settings.redis_cluster_nodes
                ]
                self.client = RedisCluster(
                    startup_nodes=startup_nodes,
                    decode_responses=False,
                    max_connections=settings.redis_max_connections
                )
                logger.info("Redis Cluster client initialized", nodes=len(startup_nodes))
            else:
                # Single Redis instance
                self.client = redis.from_url(
                    settings.redis_url,
                    decode_responses=False,
                    max_connections=settings.redis_max_connections,
                    health_check_interval=settings.redis_health_check_interval
                )
                logger.info("Redis client initialized", url=settings.redis_url)
            
            # Тестовое подключение
            await self.client.ping()
            self.initialized = True
            
        except Exception as e:
            logger.error("Failed to initialize Redis backend", error=str(e))
            raise
    
    def _serialize_value(self, value: Any) -> bytes:
        """Сериализация значения"""
        try:
            # Используем msgpack для эффективной сериализации
            return msgpack.packb(value, use_bin_type=True)
        except:
            # Fallback на pickle
            return pickle.dumps(value)
    
    def _deserialize_value(self, data: bytes) -> Any:
        """Десериализация значения"""
        if data is None:
            return None
        
        try:
            # Пробуем msgpack
            return msgpack.unpackb(data, raw=False)
        except:
            try:
                # Fallback на pickle
                return pickle.loads(data)
            except:
                # Возвращаем как есть
                return data
    
    async def get(self, key: str) -> Optional[Any]:
        """Получение значения из Redis"""
        if not self.initialized:
            return None
        
        try:
            data = await self.client.get(key)
            if data:
                return self._deserialize_value(data)
        except Exception as e:
            logger.error("Redis get error", key=key, error=str(e))
        
        return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Сохранение значения в Redis"""
        if not self.initialized:
            return False
        
        try:
            serialized = self._serialize_value(value)
            
            if ttl:
                await self.client.setex(key, ttl, serialized)
            else:
                await self.client.set(key, serialized)
            
            # Сохранение тегов если есть
            if tags:
                tag_key = f"{key}:tags"
                await self.client.sadd(tag_key, *tags)
                if ttl:
                    await self.client.expire(tag_key, ttl)
            
            return True
            
        except Exception as e:
            logger.error("Redis set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Удаление значения из Redis"""
        if not self.initialized:
            return False
        
        try:
            result = await self.client.delete(key)
            # Удаляем связанные теги
            await self.client.delete(f"{key}:tags")
            return result > 0
        except Exception as e:
            logger.error("Redis delete error", key=key, error=str(e))
            return False
    
    async def exists(self, key: str) -> bool:
        """Проверка существования ключа в Redis"""
        if not self.initialized:
            return False
        
        try:
            return await self.client.exists(key) > 0
        except Exception as e:
            logger.error("Redis exists error", key=key, error=str(e))
            return False
    
    async def mget(self, keys: List[str]) -> Dict[str, Optional[Any]]:
        """Пакетное получение значений из Redis"""
        if not self.initialized:
            return {key: None for key in keys}
        
        try:
            values = await self.client.mget(keys)
            result = {}
            
            for key, value in zip(keys, values):
                if value is not None:
                    result[key] = self._deserialize_value(value)
                else:
                    result[key] = None
            
            return result
            
        except Exception as e:
            logger.error("Redis mget error", keys=keys, error=str(e))
            return {key: None for key in keys}
    
    async def mset(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        tags: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """Пакетное сохранение значений в Redis"""
        if not self.initialized:
            return False
        
        try:
            # Сериализация значений
            serialized_items = {
                key: self._serialize_value(value)
                for key, value in items.items()
            }
            
            # Пакетное сохранение
            pipeline = self.client.pipeline()
            
            for key, value in serialized_items.items():
                if ttl:
                    pipeline.setex(key, ttl, value)
                else:
                    pipeline.set(key, value)
                
                # Сохранение тегов если есть
                if tags and key in tags:
                    tag_key = f"{key}:tags"
                    pipeline.sadd(tag_key, *tags[key])
                    if ttl:
                        pipeline.expire(tag_key, ttl)
            
            await pipeline.execute()
            return True
            
        except Exception as e:
            logger.error("Redis mset error", error=str(e))
            return False
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """Получение ключей из Redis по паттерну"""
        if not self.initialized:
            return []
        
        try:
            if isinstance(self.client, RedisCluster):
                # Redis Cluster требует другого подхода
                keys = []
                for node in self.client.get_primaries():
                    async for key in self.client.scan_iter(match=pattern, _node=node):
                        keys.append(key.decode() if isinstance(key, bytes) else key)
                return keys
            else:
                # Обычный Redis
                keys = []
                async for key in self.client.scan_iter(match=pattern):
                    keys.append(key.decode() if isinstance(key, bytes) else key)
                return keys
                
        except Exception as e:
            logger.error("Redis keys error", pattern=pattern, error=str(e))
            return []
    
    async def clear(self, pattern: str = "*") -> int:
        """Очистка кэша в Redis по паттерну"""
        if not self.initialized:
            return 0
        
        try:
            keys = await self.keys(pattern)
            if keys:
                await self.client.delete(*keys)
            return len(keys)
            
        except Exception as e:
            logger.error("Redis clear error", pattern=pattern, error=str(e))
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики Redis"""
        if not self.initialized:
            return {"error": "Redis not initialized"}
        
        try:
            info = await self.client.info()
            
            if isinstance(self.client, RedisCluster):
                # Для кластера собираем статистику со всех нод
                cluster_info = {}
                for node in self.client.get_primaries():
                    node_info = await self.client.info(_node=node)
                    cluster_info[node.name] = {
                        "keys": node_info.get("db0", {}).get("keys", 0),
                        "memory": node_info.get("used_memory_human", "0"),
                        "connected_clients": node_info.get("connected_clients", 0)
                    }
                
                return {
                    "type": "redis_cluster",
                    "nodes": len(self.client.get_primaries()),
                    "cluster_info": cluster_info,
                    "total_keys": sum(node["keys"] for node in cluster_info.values())
                }
            else:
                # Одиночный инстанс
                return {
                    "type": "redis",
                    "version": info.get("redis_version"),
                    "used_memory_human": info.get("used_memory_human"),
                    "total_keys": info.get("db0", {}).get("keys", 0),
                    "connected_clients": info.get("connected_clients", 0),
                    "hit_rate": info.get("keyspace_hits", 0) / max(
                        info.get("keyspace_misses", 0) + info.get("keyspace_hits", 1), 1
                    )
                }
                
        except Exception as e:
            logger.error("Redis stats error", error=str(e))
            return {"error": str(e)}
    
    async def shutdown(self):
        """Завершение работы Redis клиента"""
        if self.client:
            await self.client.close()
            logger.info("Redis client closed")


class MemoryBackend(CacheBackendInterface):
    """In-memory бэкенд для кэша (для тестирования)"""
    
    def __init__(self):
        self.cache: Dict[str, CacheItem] = {}
        self.max_size = settings.max_cache_size_mb * 1024 * 1024  # в байтах
        self.current_size = 0
        self.initialized = False
    
    async def initialize(self):
        """Инициализация in-memory кэша"""
        self.initialized = True
        logger.info("Memory backend initialized", max_size_mb=settings.max_cache_size_mb)
    
    def _get_item_size(self, item: CacheItem) -> int:
        """Получение размера элемента в байтах"""
        try:
            return len(pickle.dumps(item.value))
        except:
            return 1024  # fallback
    
    async def _evict_if_needed(self):
        """Вытеснение элементов если превышен лимит"""
        if self.current_size <= self.max_size:
            return
        
        # Применяем стратегию вытеснения
        if settings.cache_strategy == CacheStrategy.LRU:
            # Least Recently Used
            items = sorted(
                self.cache.items(),
                key=lambda x: x[1].accessed_at
            )
        elif settings.cache_strategy == CacheStrategy.LFU:
            # Least Frequently Used
            items = sorted(
                self.cache.items(),
                key=lambda x: x[1].access_count
            )
        elif settings.cache_strategy == CacheStrategy.FIFO:
            # First In First Out
            items = sorted(
                self.cache.items(),
                key=lambda x: x[1].created_at
            )
        else:
            # Random
            import random
            items = list(self.cache.items())
            random.shuffle(items)
        
        # Удаляем пока не освободим достаточно места
        target_size = self.max_size * 0.8  # Цель - 80% от максимума
        while self.current_size > target_size and items:
            key, item = items.pop(0)
            item_size = self._get_item_size(item)
            del self.cache[key]
            self.current_size -= item_size
    
    async def get(self, key: str) -> Optional[Any]:
        """Получение значения из памяти"""
        if not self.initialized:
            return None
        
        if key in self.cache:
            item = self.cache[key]
            
            # Проверка на expiration
            if item.is_expired():
                del self.cache[key]
                self.current_size -= self._get_item_size(item)
                return None
            
            # Обновляем статистику доступа
            item.accessed_at = time.time()
            item.access_count += 1
            
            return item.value
        
        return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Сохранение значения в памяти"""
        if not self.initialized:
            return False
        
        try:
            # Проверяем размер
            item = CacheItem(key, value, ttl, tags=tags)
            item_size = self._get_item_size(item)
            
            if item_size > settings.max_item_size_kb * 1024:
                logger.warning("Item too large", key=key, size_kb=item_size/1024)
                return False
            
            # Удаляем старый элемент если есть
            if key in self.cache:
                old_item = self.cache[key]
                old_size = self._get_item_size(old_item)
                self.current_size -= old_size
            
            # Добавляем новый
            self.cache[key] = item
            self.current_size += item_size
            
            # Вытесняем если нужно
            await self._evict_if_needed()
            
            return True
            
        except Exception as e:
            logger.error("Memory set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Удаление значения из памяти"""
        if not self.initialized:
            return False
        
        if key in self.cache:
            item = self.cache[key]
            item_size = self._get_item_size(item)
            del self.cache[key]
            self.current_size -= item_size
            return True
        
        return False
    
    async def exists(self, key: str) -> bool:
        """Проверка существования ключа в памяти"""
        if not self.initialized:
            return False
        
        if key in self.cache:
            item = self.cache[key]
            if not item.is_expired():
                return True
            else:
                # Удаляем просроченный
                await self.delete(key)
        
        return False
    
    async def mget(self, keys: List[str]) -> Dict[str, Optional[Any]]:
        """Пакетное получение значений из памяти"""
        result = {}
        for key in keys:
            result[key] = await self.get(key)
        return result
    
    async def mset(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        tags: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """Пакетное сохранение значений в памяти"""
        success = True
        for key, value in items.items():
            item_tags = tags.get(key, []) if tags else None
            if not await self.set(key, value, ttl, item_tags):
                success = False
        return success
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """Получение ключей из памяти по паттерну"""
        if not self.initialized:
            return []
        
        # Простая реализация паттерна
        import fnmatch
        current_keys = list(self.cache.keys())
        
        # Фильтрация просроченных
        valid_keys = []
        for key in current_keys:
            if await self.exists(key):
                if fnmatch.fnmatch(key, pattern):
                    valid_keys.append(key)
        
        return valid_keys
    
    async def clear(self, pattern: str = "*") -> int:
        """Очистка кэша в памяти по паттерну"""
        if not self.initialized:
            return 0
        
        keys_to_delete = await self.keys(pattern)
        for key in keys_to_delete:
            await self.delete(key)
        
        return len(keys_to_delete)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики памяти"""
        if not self.initialized:
            return {"error": "Memory backend not initialized"}
        
        # Очищаем просроченные
        expired_count = 0
        keys = list(self.cache.keys())
        for key in keys:
            if key in self.cache and self.cache[key].is_expired():
                await self.delete(key)
                expired_count += 1
        
        return {
            "type": "memory",
            "total_items": len(self.cache),
            "current_size_mb": self.current_size / 1024 / 1024,
            "max_size_mb": self.max_size / 1024 / 1024,
            "usage_percentage": (self.current_size / self.max_size * 100) if self.max_size > 0 else 0,
            "expired_cleared": expired_count,
            "cache_strategy": settings.cache_strategy.value
        }
    
    async def shutdown(self):
        """Завершение работы in-memory кэша"""
        self.cache.clear()
        self.current_size = 0
        logger.info("Memory backend cleared")


class MongoBackend(CacheBackendInterface):
    """MongoDB бэкенд для persistent кэша"""
    
    def __init__(self):
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self.collection = None
        self.initialized = False
    
    async def initialize(self):
        """Инициализация MongoDB клиента"""
        try:
            self.client = MongoClient(
                settings.mongo_url,
                serverSelectionTimeoutMS=5000
            )
            
            # Проверка подключения
            self.client.admin.command('ping')
            
            self.db = self.client[settings.mongo_database]
            self.collection = self.db[settings.mongo_collection]
            
            # Создание индексов
            await self._create_indexes()
            
            self.initialized = True
            logger.info("MongoDB backend initialized", url=settings.mongo_url)
            
        except Exception as e:
            logger.error("Failed to initialize MongoDB backend", error=str(e))
            raise
    
    async def _create_indexes(self):
        """Создание необходимых индексов"""
        # В реальном приложении здесь был бы async код
        # Для простоты используем синхронный вызов
        self.collection.create_index([("key", 1)], unique=True)
        self.collection.create_index([("expires_at", 1)], expireAfterSeconds=0)
        self.collection.create_index([("tags", 1)])
        self.collection.create_index([("accessed_at", -1)])
        self.collection.create_index([("created_at", -1)])
    
    async def get(self, key: str) -> Optional[Any]:
        """Получение значения из MongoDB"""
        if not self.initialized:
            return None
        
        try:
            doc = self.collection.find_one({"key": key})
            
            if not doc:
                return None
            
            # Проверка на expiration
            if doc.get("expires_at") and doc["expires_at"] < datetime.utcnow():
                self.collection.delete_one({"_id": doc["_id"]})
                return None
            
            # Обновляем время доступа
            self.collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {"accessed_at": datetime.utcnow()},
                    "$inc": {"access_count": 1}
                }
            )
            
            return doc["value"]
            
        except Exception as e:
            logger.error("MongoDB get error", key=key, error=str(e))
            return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Сохранение значения в MongoDB"""
        if not self.initialized:
            return False
        
        try:
            expires_at = None
            if ttl:
                expires_at = datetime.utcnow() + timedelta(seconds=ttl)
            
            doc = {
                "key": key,
                "value": value,
                "tags": tags or [],
                "created_at": datetime.utcnow(),
                "accessed_at": datetime.utcnow(),
                "access_count": 0,
                "expires_at": expires_at
            }
            
            # Upsert операция
            self.collection.replace_one(
                {"key": key},
                doc,
                upsert=True
            )
            
            return True
            
        except Exception as e:
            logger.error("MongoDB set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Удаление значения из MongoDB"""
        if not self.initialized:
            return False
        
        try:
            result = self.collection.delete_one({"key": key})
            return result.deleted_count > 0
        except Exception as e:
            logger.error("MongoDB delete error", key=key, error=str(e))
            return False
    
    async def exists(self, key: str) -> bool:
        """Проверка существования ключа в MongoDB"""
        if not self.initialized:
            return False
        
        try:
            doc = self.collection.find_one({"key": key})
            if not doc:
                return False
            
            # Проверка на expiration
            if doc.get("expires_at") and doc["expires_at"] < datetime.utcnow():
                self.collection.delete_one({"_id": doc["_id"]})
                return False
            
            return True
            
        except Exception as e:
            logger.error("MongoDB exists error", key=key, error=str(e))
            return False
    
    async def mget(self, keys: List[str]) -> Dict[str, Optional[Any]]:
        """Пакетное получение значений из MongoDB"""
        if not self.initialized:
            return {key: None for key in keys}
        
        try:
            cursor = self.collection.find({"key": {"$in": keys}})
            docs = {doc["key"]: doc for doc in cursor}
            
            result = {}
            now = datetime.utcnow()
            
            for key in keys:
                if key in docs:
                    doc = docs[key]
                    
                    # Проверка на expiration
                    if doc.get("expires_at") and doc["expires_at"] < now:
                        self.collection.delete_one({"_id": doc["_id"]})
                        result[key] = None
                    else:
                        # Обновляем время доступа
                        self.collection.update_one(
                            {"_id": doc["_id"]},
                            {
                                "$set": {"accessed_at": now},
                                "$inc": {"access_count": 1}
                            }
                        )
                        result[key] = doc["value"]
                else:
                    result[key] = None
            
            return result
            
        except Exception as e:
            logger.error("MongoDB mget error", keys=keys, error=str(e))
            return {key: None for key in keys}
    
    async def mset(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        tags: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """Пакетное сохранение значений в MongoDB"""
        if not self.initialized:
            return False
        
        try:
            now = datetime.utcnow()
            expires_at = None
            if ttl:
                expires_at = now + timedelta(seconds=ttl)
            
            operations = []
            for key, value in items.items():
                item_tags = tags.get(key, []) if tags else []
                
                doc = {
                    "key": key,
                    "value": value,
                    "tags": item_tags,
                    "created_at": now,
                    "accessed_at": now,
                    "access_count": 0,
                    "expires_at": expires_at
                }
                
                operations.append(
                    pymongo.ReplaceOne(
                        {"key": key},
                        doc,
                        upsert=True
                    )
                )
            
            if operations:
                self.collection.bulk_write(operations)
            
            return True
            
        except Exception as e:
            logger.error("MongoDB mset error", error=str(e))
            return False
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """Получение ключей из MongoDB по паттерну"""
        if not self.initialized:
            return []
        
        try:
            # MongoDB не поддерживает wildcard в find для ключей
            # Используем regex для простых паттернов
            regex_pattern = pattern.replace("*", ".*")
            cursor = self.collection.find(
                {"key": {"$regex": f"^{regex_pattern}$"}},
                {"key": 1}
            )
            
            keys = [doc["key"] for doc in cursor]
            return keys
            
        except Exception as e:
            logger.error("MongoDB keys error", pattern=pattern, error=str(e))
            return []
    
    async def clear(self, pattern: str = "*") -> int:
        """Очистка кэша в MongoDB по паттерну"""
        if not self.initialized:
            return 0
        
        try:
            regex_pattern = pattern.replace("*", ".*")
            result = self.collection.delete_many(
                {"key": {"$regex": f"^{regex_pattern}$"}}
            )
            return result.deleted_count
            
        except Exception as e:
            logger.error("MongoDB clear error", pattern=pattern, error=str(e))
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики MongoDB"""
        if not self.initialized:
            return {"error": "MongoDB not initialized"}
        
        try:
            stats = self.db.command("dbstats")
            coll_stats = self.db.command("collstats", settings.mongo_collection)
            
            # Очищаем просроченные
            expired_count = self.collection.delete_many(
                {"expires_at": {"$lt": datetime.utcnow()}}
            ).deleted_count
            
            return {
                "type": "mongodb",
                "database": settings.mongo_database,
                "collection": settings.mongo_collection,
                "total_documents": coll_stats.get("count", 0),
                "size_mb": coll_stats.get("size", 0) / 1024 / 1024,
                "storage_size_mb": coll_stats.get("storageSize", 0) / 1024 / 1024,
                "index_size_mb": coll_stats.get("totalIndexSize", 0) / 1024 / 1024,
                "expired_cleared": expired_count,
                "database_stats": {
                    "collections": stats.get("collections", 0),
                    "objects": stats.get("objects", 0),
                    "data_size_mb": stats.get("dataSize", 0) / 1024 / 1024
                }
            }
            
        except Exception as e:
            logger.error("MongoDB stats error", error=str(e))
            return {"error": str(e)}
    
    async def shutdown(self):
        """Завершение работы MongoDB клиента"""
        if self.client:
            self.client.close()
            logger.info("MongoDB client closed")


class CacheManager:
    """Менеджер кэша с поддержкой multiple backends"""
    
    def __init__(self):
        self.backend: Optional[CacheBackendInterface] = None
        self.metrics: Dict[str, Any] = {
            "operations": {"get": 0, "set": 0, "delete": 0, "hit": 0, "miss": 0},
            "timings": {"get": [], "set": [], "delete": []},
            "errors": {"get": 0, "set": 0, "delete": 0}
        }
    
    async def initialize(self):
        """Инициализация менеджера кэша"""
        # Выбор бэкенда
        if settings.cache_backend == CacheBackend.REDIS:
            self.backend = RedisBackend()
        elif settings.cache_backend == CacheBackend.REDIS_CLUSTER:
            self.backend = RedisBackend()
        elif settings.cache_backend == CacheBackend.MEMORY:
            self.backend = MemoryBackend()
        elif settings.cache_backend == CacheBackend.MONGO:
            self.backend = MongoBackend()
        else:
            raise ValueError(f"Unsupported cache backend: {settings.cache_backend}")
        
        await self.backend.initialize()
        logger.info(
            "Cache manager initialized",
            backend=settings.cache_backend.value,
            strategy=settings.cache_strategy.value
        )
    
    async def get(self, key: str) -> Optional[Any]:
        """Получение значения из кэша"""
        start_time = time.time()
        self.metrics["operations"]["get"] += 1
        
        try:
            value = await self.backend.get(key)
            elapsed = time.time() - start_time
            self.metrics["timings"]["get"].append(elapsed)
            
            if value is not None:
                self.metrics["operations"]["hit"] += 1
                logger.debug("Cache hit", key=key, elapsed=elapsed)
            else:
                self.metrics["operations"]["miss"] += 1
                logger.debug("Cache miss", key=key, elapsed=elapsed)
            
            return value
            
        except Exception as e:
            self.metrics["errors"]["get"] += 1
            logger.error("Cache get error", key=key, error=str(e))
            return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> bool:
        """Сохранение значения в кэш"""
        start_time = time.time()
        self.metrics["operations"]["set"] += 1
        
        try:
            success = await self.backend.set(key, value, ttl, tags)
            elapsed = time.time() - start_time
            self.metrics["timings"]["set"].append(elapsed)
            
            if success:
                logger.debug("Cache set", key=key, ttl=ttl, elapsed=elapsed)
            else:
                logger.warning("Cache set failed", key=key)
            
            return success
            
        except Exception as e:
            self.metrics["errors"]["set"] += 1
            logger.error("Cache set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Удаление значения из кэша"""
        start_time = time.time()
        self.metrics["operations"]["delete"] += 1
        
        try:
            success = await self.backend.delete(key)
            elapsed = time.time() - start_time
            self.metrics["timings"]["delete"].append(elapsed)
            
            logger.debug("Cache delete", key=key, success=success, elapsed=elapsed)
            return success
            
        except Exception as e:
            self.metrics["errors"]["delete"] += 1
            logger.error("Cache delete error", key=key, error=str(e))
            return False
    
    async def exists(self, key: str) -> bool:
        """Проверка существования ключа"""
        try:
            return await self.backend.exists(key)
        except Exception as e:
            logger.error("Cache exists error", key=key, error=str(e))
            return False
    
    async def mget(self, keys: List[str]) -> Dict[str, Optional[Any]]:
        """Пакетное получение значений"""
        try:
            return await self.backend.mget(keys)
        except Exception as e:
            logger.error("Cache mget error", keys=keys, error=str(e))
            return {key: None for key in keys}
    
    async def mset(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        tags: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """Пакетное сохранение значений"""
        try:
            return await self.backend.mset(items, ttl, tags)
        except Exception as e:
            logger.error("Cache mset error", error=str(e))
            return False
    
    async def get_with_fallback(
        self,
        key: str,
        fallback_func,
        ttl: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> Any:
        """Получение значения с fallback функцией"""
        # Пробуем получить из кэша
        cached = await self.get(key)
        if cached is not None:
            return cached
        
        # Если нет в кэше, вызываем fallback функцию
        try:
            value = await fallback_func()
            
            # Сохраняем в кэш
            if value is not None:
                await self.set(key, value, ttl, tags)
            
            return value
            
        except Exception as e:
            logger.error("Fallback function error", key=key, error=str(e))
            raise
    
    async def invalidate_by_tags(self, tags: List[str]) -> int:
        """Инвалидация кэша по тегам"""
        try:
            count = 0
            
            if isinstance(self.backend, RedisBackend):
                # Для Redis ищем ключи по тегам
                for tag in tags:
                    # Здесь нужна специальная логика для поиска по тегам
                    # В Redis теги хранятся в отдельных множествах
                    pass
            elif isinstance(self.backend, MongoBackend):
                # Для MongoDB
                result = self.backend.collection.delete_many(
                    {"tags": {"$in": tags}}
                )
                count = result.deleted_count
            else:
                # Для других бэкендов перебираем все ключи
                all_keys = await self.backend.keys()
                for key in all_keys:
                    item = await self.backend.get(key)
                    if hasattr(item, 'tags'):
                        if any(tag in item.tags for tag in tags):
                            await self.backend.delete(key)
                            count += 1
            
            logger.info("Cache invalidated by tags", tags=tags, count=count)
            return count
            
        except Exception as e:
            logger.error("Invalidate by tags error", tags=tags, error=str(e))
            return 0
    
    async def keys(self, pattern: str = "*") -> List[str]:
        """Получение ключей по паттерну"""
        try:
            return await self.backend.keys(pattern)
        except Exception as e:
            logger.error("Cache keys error", pattern=pattern, error=str(e))
            return []
    
    async def clear(self, pattern: str = "*") -> int:
        """Очистка кэша по паттерну"""
        try:
            count = await self.backend.clear(pattern)
            logger.info("Cache cleared", pattern=pattern, count=count)
            return count
        except Exception as e:
            logger.error("Cache clear error", pattern=pattern, error=str(e))
            return 0
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики кэша"""
        try:
            backend_stats = await self.backend.get_stats()
            
            # Рассчитываем метрики
            total_gets = self.metrics["operations"]["get"]
            hits = self.metrics["operations"]["hit"]
            hit_rate = (hits / total_gets * 100) if total_gets > 0 else 0
            
            # Среднее время операций
            avg_timings = {}
            for op, timings in self.metrics["timings"].items():
                if timings:
                    avg_timings[op] = sum(timings) / len(timings)
                else:
                    avg_timings[op] = 0
            
            # Обрезаем историю таймингов
            for op in self.metrics["timings"]:
                if len(self.metrics["timings"][op]) > 1000:
                    self.metrics["timings"][op] = self.metrics["timings"][op][-1000:]
            
            return {
                "backend": backend_stats,
                "operations": self.metrics["operations"],
                "avg_timings_ms": {k: v * 1000 for k, v in avg_timings.items()},
                "hit_rate_percent": hit_rate,
                "error_count": self.metrics["errors"],
                "uptime": time.time() - self.start_time if hasattr(self, 'start_time') else 0
            }
            
        except Exception as e:
            logger.error("Get stats error", error=str(e))
            return {"error": str(e)}
    
    async def shutdown(self):
        """Завершение работы менеджера кэша"""
        if self.backend:
            await self.backend.shutdown()
            logger.info("Cache manager shutdown complete")