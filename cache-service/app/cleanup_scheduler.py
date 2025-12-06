# C:\Users\user\Desktop\TechStats\cache-service\app\cleanup_scheduler.py
import asyncio
import time
from typing import Optional, Dict, Any
import structlog

from app.cache_manager import CacheManager

logger = structlog.get_logger()


class CleanupScheduler:
    """Планировщик для регулярной очистки кэша"""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self.task: Optional[asyncio.Task] = None
        self.running = False
        self.stats: Dict[str, Any] = {
            "cleanups_performed": 0,
            "total_keys_cleared": 0,
            "last_cleanup": None,
            "errors": 0
        }
    
    async def start(self):
        """Запуск планировщика"""
        if self.running:
            logger.warning("Cleanup scheduler already running")
            return
        
        self.running = True
        self.task = asyncio.create_task(self._cleanup_loop())
        logger.info("Cleanup scheduler started", interval=settings.cleanup_interval_seconds)
    
    async def stop(self):
        """Остановка планировщика"""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        logger.info("Cleanup scheduler stopped")
    
    async def _cleanup_loop(self):
        """Цикл очистки"""
        while self.running:
            try:
                await asyncio.sleep(settings.cleanup_interval_seconds)
                await self._perform_cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.stats["errors"] += 1
                logger.error("Cleanup loop error", error=str(e))
                await asyncio.sleep(60)  # Ждем перед повторной попыткой
    
    async def _perform_cleanup(self):
        """Выполнение очистки"""
        logger.debug("Starting scheduled cleanup")
        start_time = time.time()
        
        try:
            # 1. Очистка по TTL (для бэкендов без автоматического TTL)
            if settings.cache_backend in [CacheBackend.MEMORY, CacheBackend.MONGO]:
                await self._cleanup_expired()
            
            # 2. Очистка по размеру (для in-memory кэша)
            if settings.cache_backend == CacheBackend.MEMORY:
                await self._cleanup_by_size()
            
            # 3. Очистка старых данных
            await self._cleanup_old_data()
            
            # 4. Сбор статистики
            await self._collect_statistics()
            
            elapsed = time.time() - start_time
            self.stats["cleanups_performed"] += 1
            self.stats["last_cleanup"] = time.time()
            
            logger.info(
                "Cleanup completed",
                duration=elapsed,
                total_cleanups=self.stats["cleanups_performed"]
            )
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error("Cleanup failed", error=str(e))
    
    async def _cleanup_expired(self):
        """Очистка просроченных элементов"""
        # Эта логика зависит от бэкенда
        # В Redis и MongoDB TTL обрабатывается автоматически
        # Для памяти нужно делать вручную
        
        if settings.cache_backend == CacheBackend.MEMORY:
            # Для in-memory кэша перебираем все элементы
            keys = await self.cache_manager.keys()
            cleared = 0
            
            for key in keys:
                # Проверяем exists, который уже проверяет expiration
                if not await self.cache_manager.exists(key):
                    cleared += 1
            
            if cleared > 0:
                logger.debug("Expired items cleanup", cleared=cleared)
                self.stats["total_keys_cleared"] += cleared
    
    async def _cleanup_by_size(self):
        """Очистка по размеру (только для in-memory)"""
        if settings.cache_backend != CacheBackend.MEMORY:
            return
        
        # Получаем статистику для проверки размера
        stats = await self.cache_manager.get_stats()
        
        if "backend" in stats and "usage_percentage" in stats["backend"]:
            usage = stats["backend"]["usage_percentage"]
            
            if usage > 90:  # Если использование > 90%
                logger.warning("High cache usage detected", usage=usage)
                
                # Находим старые или редко используемые элементы
                # Это зависит от стратегии кэширования
                
    async def _cleanup_old_data(self):
        """Очистка очень старых данных"""
        # Находим ключи с определенными паттернами, которые очень старые
        patterns_to_clean = [
            "temp:*",
            "session:*",
            "lock:*",
            "rate_limit:*"
        ]
        
        for pattern in patterns_to_clean:
            try:
                # Можно добавить дополнительную логику для проверки возраста
                cleared = await self.cache_manager.clear(pattern)
                if cleared > 0:
                    logger.debug("Old data cleanup", pattern=pattern, cleared=cleared)
                    self.stats["total_keys_cleared"] += cleared
            except Exception as e:
                logger.warning("Pattern cleanup failed", pattern=pattern, error=str(e))
    
    async def _collect_statistics(self):
        """Сбор статистики для мониторинга"""
        try:
            stats = await self.cache_manager.get_stats()
            
            # Логируем важные метрики
            if "backend" in stats and "total_items" in stats["backend"]:
                total_items = stats["backend"]["total_items"]
                hit_rate = stats.get("hit_rate_percent", 0)
                
                if total_items > 10000:
                    logger.info(
                        "Cache statistics",
                        total_items=total_items,
                        hit_rate=hit_rate
                    )
        except Exception as e:
            logger.warning("Statistics collection failed", error=str(e))
    
    async def get_stats(self) -> Dict[str, Any]:
        """Получение статистики планировщика"""
        return self.stats.copy()