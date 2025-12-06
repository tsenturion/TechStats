# C:\Users\user\Desktop\TechStats\analyzer-service\app\cache.py
import json
import hashlib
from typing import Optional, Any, Dict, List
import redis.asyncio as redis
import structlog

from config import settings

logger = structlog.get_logger()


class CacheManager:
    """Менеджер кэширования для результатов анализа"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
    
    async def init_redis(self):
        """Инициализация Redis"""
        self.redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=False
        )
    
    def _generate_analysis_key(
        self,
        vacancy_ids: List[str],
        technology: str,
        exact_search: bool
    ) -> str:
        """Генерация ключа для кэша анализа"""
        # Сортируем ID для консистентности
        sorted_ids = sorted(vacancy_ids)
        ids_hash = hashlib.md5(','.join(sorted_ids).encode()).hexdigest()[:16]
        
        return f"analysis:{ids_hash}:{technology}:{exact_search}"
    
    def _generate_vacancy_analysis_key(
        self,
        vacancy_id: str,
        technology: str,
        exact_search: bool
    ) -> str:
        """Генерация ключа для анализа конкретной вакансии"""
        return f"vacancy_analysis:{vacancy_id}:{technology}:{exact_search}"
    
    async def get_analysis_result(
        self,
        vacancy_ids: List[str],
        technology: str,
        exact_search: bool
    ) -> Optional[Dict[str, Any]]:
        """Получение результата анализа из кэша"""
        if not self.redis_client:
            return None
        
        key = self._generate_analysis_key(vacancy_ids, technology, exact_search)
        
        try:
            data = await self.redis_client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning("Cache get error", key=key, error=str(e))
        
        return None
    
    async def cache_analysis_result(
        self,
        vacancy_ids: List[str],
        technology: str,
        exact_search: bool,
        result: Dict[str, Any]
    ) -> bool:
        """Кэширование результата анализа"""
        if not self.redis_client:
            return False
        
        key = self._generate_analysis_key(vacancy_ids, technology, exact_search)
        
        try:
            await self.redis_client.set(
                key,
                json.dumps(result, ensure_ascii=False),
                ex=settings.analysis_cache_ttl_hours * 3600
            )
            return True
        except Exception as e:
            logger.error("Cache set error", key=key, error=str(e))
            return False
    
    async def get_vacancy_analysis(
        self,
        vacancy_id: str,
        technology: str,
        exact_search: bool
    ) -> Optional[Dict[str, Any]]:
        """Получение анализа конкретной вакансии из кэша"""
        if not self.redis_client:
            return None
        
        key = self._generate_vacancy_analysis_key(vacancy_id, technology, exact_search)
        
        try:
            data = await self.redis_client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning("Vacancy cache get error", key=key, error=str(e))
        
        return None
    
    async def cache_vacancy_analysis(
        self,
        vacancy_id: str,
        technology: str,
        exact_search: bool,
        result: Dict[str, Any]
    ) -> bool:
        """Кэширование анализа конкретной вакансии"""
        if not self.redis_client:
            return False
        
        key = self._generate_vacancy_analysis_key(vacancy_id, technology, exact_search)
        
        try:
            await self.redis_client.set(
                key,
                json.dumps(result, ensure_ascii=False),
                ex=settings.analysis_cache_ttl_hours * 3600
            )
            return True
        except Exception as e:
            logger.error("Vacancy cache set error", key=key, error=str(e))
            return False
    
    async def get_batch_analysis(
        self,
        vacancy_ids: List[str],
        technology: str,
        exact_search: bool
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Пакетное получение анализов из кэша"""
        if not self.redis_client:
            return {}
        
        try:
            keys = [
                self._generate_vacancy_analysis_key(vid, technology, exact_search)
                for vid in vacancy_ids
            ]
            
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
    
    async def cache_batch_analysis(
        self,
        analyses: List[Dict[str, Any]],
        technology: str,
        exact_search: bool
    ) -> bool:
        """Пакетное кэширование анализов"""
        if not self.redis_client:
            return False
        
        try:
            pipeline = self.redis_client.pipeline()
            
            for analysis in analyses:
                vacancy_id = analysis.get("vacancy_id")
                if vacancy_id:
                    key = self._generate_vacancy_analysis_key(
                        vacancy_id,
                        technology,
                        exact_search
                    )
                    pipeline.set(
                        key,
                        json.dumps(analysis, ensure_ascii=False),
                        ex=settings.analysis_cache_ttl_hours * 3600
                    )
            
            await pipeline.execute()
            return True
        except Exception as e:
            logger.error("Batch cache set error", error=str(e))
            return False
    
    async def clear_analysis_cache(self, pattern: str = "analysis:*") -> int:
        """Очистка кэша анализов"""
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
            analysis_keys = await self.redis_client.keys("analysis:*")
            vacancy_analysis_keys = await self.redis_client.keys("vacancy_analysis:*")
            pattern_keys = await self.redis_client.keys("tech_patterns:*")
            
            return {
                "redis_version": info.get("redis_version"),
                "used_memory_human": info.get("used_memory_human"),
                "total_keys": keys,
                "analysis_keys": len(analysis_keys),
                "vacancy_analysis_keys": len(vacancy_analysis_keys),
                "pattern_keys": len(pattern_keys),
                "hit_rate": info.get("keyspace_hits", 0) / max(
                    info.get("keyspace_misses", 0) + info.get("keyspace_hits", 1), 1
                )
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