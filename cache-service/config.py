# C:\Users\user\Desktop\TechStats\cache-service\config.py
import json
from typing import List, Optional, Dict, Any, Union
from enum import Enum
import os
from pydantic_settings import BaseSettings
from pydantic import field_validator, Field

class CacheBackend(str, Enum):
    REDIS = "redis"
    REDIS_CLUSTER = "redis_cluster"
    MEMORY = "memory"
    MONGO = "mongo"


class CacheStrategy(str, Enum):
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    RANDOM = "random"


class Settings(BaseSettings):
    # Основные настройки
    app_name: str = "TechStats Cache Service"
    debug: bool = True
    environment: str = "production"
    version: str = "1.0.0"

    # Redis настройки
    redis_url: str = "redis://redis:6379"
    redis_max_connections: int = 100
    redis_health_check_interval: int = 30
    redis_password: Optional[str] = None
    redis_db: int = 0

    # Redis Cluster настройки (опционально)
    redis_cluster_mode: bool = False
    redis_cluster_nodes: str = ""

    # MongoDB настройки (для persistent cache)
    mongo_enabled: bool = False
    mongo_url: str = "mongodb://mongo:27017"
    mongo_database: str = "cache_db"
    mongo_collection: str = "cache_items"

    # Выбор бэкенда
    cache_backend: CacheBackend = CacheBackend.REDIS
    cache_strategy: CacheStrategy = CacheStrategy.LRU

    # Настройки кэша
    default_ttl_seconds: int = 3600  # 1 час по умолчанию
    max_cache_size_mb: int = 1024    # 1 GB max
    max_item_size_kb: int = 1024     # 1 MB max на элемент

    # Настройки очистки
    cleanup_interval_seconds: int = 300  # 5 минут
    cleanup_batch_size: int = 1000

    # Настройки кластера
    enable_clustering: bool = False
    cluster_nodes: str = ""
    node_id: str = "cache-node-1"

    # Rate limiting для API
    api_rate_limit_per_minute: int = 1000
    api_rate_limit_per_hour: int = 10000

    # Service settings
    port: int = 8003
    workers: int = 4
    log_level: str = "info"

    # Monitoring
    enable_prometheus: bool = True
    metrics_port: int = 9091

    # Административный доступ
    admin_token: str = "admin_secret_token"

    class Config:
        env_file = ".env"

    def get_redis_cluster_nodes(self) -> List[str]:
        """Получение списка нод Redis кластера"""
        if not self.redis_cluster_nodes or self.redis_cluster_nodes.strip() == "":
            return []
        
        # Пробуем парсить как JSON
        try:
            if self.redis_cluster_nodes.strip().startswith('['):
                return json.loads(self.redis_cluster_nodes)
        except json.JSONDecodeError:
            pass
        
        # Разбиваем по запятой
        nodes = [node.strip() for node in self.redis_cluster_nodes.split(',') if node.strip()]
        return nodes

    def get_cluster_nodes(self) -> List[str]:
        """Получение списка нод кластера"""
        if not self.cluster_nodes or self.cluster_nodes.strip() == "":
            return []
        
        # Пробуем парсить как JSON
        try:
            if self.cluster_nodes.strip().startswith('['):
                return json.loads(self.cluster_nodes)
        except json.JSONDecodeError:
            pass
        
        # Разбиваем по запятой
        nodes = [node.strip() for node in self.cluster_nodes.split(',') if node.strip()]
        return nodes


settings = Settings()