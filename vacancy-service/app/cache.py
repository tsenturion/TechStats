# C:\Users\user\Desktop\TechStats\vacancy-service\app\cache.py
import json
import hashlib
from typing import Optional, Any, Dict, List
from datetime import datetime, timedelta
import redis.asyncio as redis
import structlog

from config import settings

logger = structlog.get_logger()


class CacheManager:
    """Менеджер кэширования для вакансий"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
    
    async def init_redis(self):
        """Инициализация Redis"""
        self.redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=False  # Для хранения JSON как bytes
        )
    
    def _generate_key(self, prefix: str, **kwargs) -> str:
        """Генерация ключа для кэша"""
        key_parts = [prefix]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        
        key_string = ":".join(key_parts)
        # Используем hash для длинных ключей
        if len(key_string) > 200:
            key_hash = hashlib.md5(key_string.encode()).hexdigest()
            return f"{prefix}:hash:{key_hash}"
        
        return key_string
    
    async def get(self, key: str) -> Optional[Any]:
        """Получение данных из кэша"""
        if not self.redis_client:
            return None
        
        try:
            data = await self.redis_client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning("Cache get error", key=key, error=str(e))
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Сохранение данных в кэш"""
        if not self.redis_client:
            return False
        
        try:
            if ttl is None:
                ttl = settings.cache_ttl_hours * 3600
            
            await self.redis_client.set(
                key,
                json.dumps(value, ensure_ascii=False),
                ex=ttl
            )
            return True
        except Exception as e:
            logger.error("Cache set error", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Удаление данных из кэша"""
        if not self.redis_client:
            return False
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error("Cache delete error", key=key, error=str(e))
            return False
    
    async def search_vacancies_cache(
        self,
        query: str,
        area: int,
        page: int,
        per_page: int,
        search_field: str
    ) -> Optional[Dict[str, Any]]:
        """Поиск вакансий в кэше"""
        key = self._generate_key(
            "vacancies:search",
            query=query,
            area=area,
            page=page,
            per_page=per_page,
            field=search_field
        )
        return await self.get(key)
    
    async def cache_search_results(
        self,
        query: str,
        area: int,
        page: int,
        per_page: int,
        search_field: str,
        results: Dict[str, Any]
    ) -> bool:
        """Кэширование результатов поиска"""
        key = self._generate_key(
            "vacancies:search",
            query=query,
            area=area,
            page=page,
            per_page=per_page,
            field=search_field
        )
        return await self.set(key, results, ttl=settings.search_cache_ttl_minutes * 60)
    
    async def get_vacancy_cache(self, vacancy_id: str) -> Optional[Dict[str, Any]]:
        """Получение вакансии из кэша"""
        key = f"vacancy:{vacancy_id}"
        return await self.get(key)
    
    async def cache_vacancy(
        self,
        vacancy_id: str,
        vacancy_data: Dict[str, Any]
    ) -> bool:
        """Кэширование информации о вакансии"""
        key = f"vacancy:{vacancy_id}"
        return await self.set(key, vacancy_data)
    
    async def get_vacancies_batch_cache(
        self,
        vacancy_ids: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Получение нескольких вакансий из кэша"""
        if not self.redis_client:
            return {}
        
        try:
            keys = [f"vacancy:{vid}" for vid in vacancy_ids]
            results = await self.redis_client.mget(keys)
            
            cached = {}
            for vacancy_id, data in zip(vacancy_ids, results):
                if data:
                    cached[vacancy_id] = json.loads(data)
                else:
                    cached[vacancy_id] = None
            
            return cached
        except Exception as e:
            logger.error("Batch cache get error", error=str(e))
            return {}
    
    async def cache_vacancies_batch(
        self,
        vacancies: List[Dict[str, Any]]
    ) -> bool:
        """Кэширование нескольких вакансий"""
        if not self.redis_client:
            return False
        
        try:
            pipeline = self.redis_client.pipeline()
            
            for vacancy in vacancies:
                vacancy_id = vacancy.get("id")
                if vacancy_id:
                    key = f"vacancy:{vacancy_id}"
                    pipeline.set(
                        key,
                        json.dumps(vacancy, ensure_ascii=False),
                        ex=settings.cache_ttl_hours * 3600
                    )
            
            await pipeline.execute()
            return True
        except Exception as e:
            logger.error("Batch cache set error", error=str(e))
            return False
    
    async def clear_cache(self, pattern: str = "*") -> int:
        """Очистка кэша по паттерну"""
        if not self.redis_client:
            return 0
        
        try:
            keys = await self.redis_client.keys(pattern)
            if keys:
                await self.redis_client.delete(*keys)
            return len(keys)
        except Exception as e:
            logger.error("Cache clear error", pattern=pattern, error=str(e))
            return 0
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Получение статистики кэша"""
        if not self.redis_client:
            return {"error": "Redis not connected"}
        
        try:
            info = await self.redis_client.info()
            keys = await self.redis_client.dbsize()
            
            # Подсчет ключей по паттернам
            search_keys = await self.redis_client.keys("vacancies:search:*")
            vacancy_keys = await self.redis_client.keys("vacancy:*")
            
            return {
                "redis_version": info.get("redis_version"),
                "used_memory_human": info.get("used_memory_human"),
                "total_keys": keys,
                "search_keys": len(search_keys),
                "vacancy_keys": len(vacancy_keys),
                "hit_rate": info.get("keyspace_hits", 0) / max(info.get("keyspace_misses", 0) + info.get("keyspace_hits", 1), 1)
            }
        except Exception as e:
            logger.error("Cache stats error", error=str(e))
            return {"error": str(e)}
    
    async def close(self):
        """Закрытие соединения с Redis"""
        if self.redis_client:
            await self.redis_client.close()


# Глобальный экземпляр cache manager
cache_manager = CacheManager()