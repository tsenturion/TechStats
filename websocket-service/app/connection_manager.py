# C:\Users\user\Desktop\TechStats\websocket-service\app\connection_manager.py
import asyncio
import time
import uuid
from typing import Dict, List, Set, Optional, Any, Tuple
from collections import defaultdict, deque
import structlog
from fastapi import WebSocket
from collections import Counter as CounterType

from config import settings

logger = structlog.get_logger()


class ConnectionManager:
    """Менеджер WebSocket соединений"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_info: Dict[str, Dict[str, Any]] = {}
        self.connection_ids: Dict[WebSocket, str] = {}
        
        # Подписки по темам
        self.subscriptions: Dict[str, Set[str]] = defaultdict(set)  # topic -> connection_ids
        
        # Статистика
        self.stats = {
            "connections_accepted": 0,
            "connections_rejected": 0,
            "messages_sent": 0,
            "messages_received": 0,
            "errors": 0
        }
        
        # Ограничения по IP
        self.connections_by_ip: Dict[str, List[str]] = defaultdict(list)
        self.ip_last_connection_time: Dict[str, float] = {}
        
        # История сообщений для отладки
        self.message_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Блокировки для потокобезопасности
        self.lock = asyncio.Lock()
        
    def generate_connection_id(self) -> str:
        """Генерация уникального ID соединения"""
        return f"conn_{uuid.uuid4().hex[:16]}"
    
    async def connect(self, websocket: WebSocket) -> str:
        """Подключение нового WebSocket соединения"""
        async with self.lock:
            client_ip = websocket.client.host if websocket.client else "unknown"
            
            # Проверка ограничений по IP
            if len(self.connections_by_ip[client_ip]) >= settings.max_connections_per_ip:
                self.stats["connections_rejected"] += 1
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many connections from this IP. Maximum: {settings.max_connections_per_ip}"
                )
            
            # Проверка общего количества соединений
            if len(self.active_connections) >= settings.max_total_connections:
                self.stats["connections_rejected"] += 1
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many active connections. Maximum: {settings.max_total_connections}"
                )
            
            # Принятие соединения
            await websocket.accept()
            
            # Генерация ID соединения
            connection_id = self.generate_connection_id()
            
            # Сохранение информации о соединении
            self.active_connections[connection_id] = websocket
            self.connection_ids[websocket] = connection_id
            
            self.connection_info[connection_id] = {
                "client_ip": client_ip,
                "connected_at": time.time(),
                "last_activity": time.time(),
                "message_count_sent": 0,
                "message_count_received": 0,
                "subscriptions": set(),
                "user_agent": websocket.headers.get("user-agent", ""),
                "path": websocket.url.path
            }
            
            # Регистрация соединения по IP
            self.connections_by_ip[client_ip].append(connection_id)
            self.ip_last_connection_time[client_ip] = time.time()
            
            self.stats["connections_accepted"] += 1
            
            logger.debug(
                "Connection registered",
                connection_id=connection_id,
                client_ip=client_ip,
                total_connections=len(self.active_connections)
            )
            
            return connection_id
    
    async def disconnect(self, websocket: WebSocket):
        """Отключение WebSocket соединения"""
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            
            if not connection_id:
                return
            
            # Удаление из активных соединений
            if connection_id in self.active_connections:
                del self.active_connections[connection_id]
            
            if websocket in self.connection_ids:
                del self.connection_ids[websocket]
            
            # Удаление подписок
            if connection_id in self.connection_info:
                subscriptions = self.connection_info[connection_id].get("subscriptions", set())
                for topic in subscriptions:
                    if connection_id in self.subscriptions[topic]:
                        self.subscriptions[topic].remove(connection_id)
                
                # Удаление информации о соединении
                del self.connection_info[connection_id]
            
            # Удаление из списка соединений по IP
            for ip, connections in self.connections_by_ip.items():
                if connection_id in connections:
                    connections.remove(connection_id)
                    if not connections:
                        del self.connections_by_ip[ip]
                    break
            
            logger.debug(
                "Connection unregistered",
                connection_id=connection_id,
                total_connections=len(self.active_connections)
            )
    
    async def disconnect_all(self):
        """Отключение всех соединений"""
        async with self.lock:
            connections_to_disconnect = list(self.active_connections.values())
            
            for websocket in connections_to_disconnect:
                try:
                    await websocket.close(code=1000, reason="Server shutdown")
                except:
                    pass
            
            self.active_connections.clear()
            self.connection_info.clear()
            self.connection_ids.clear()
            self.subscriptions.clear()
            self.connections_by_ip.clear()
            
            logger.info("All connections disconnected")
    
    async def cleanup_inactive_connections(self):
        """Очистка неактивных соединений"""
        async with self.lock:
            current_time = time.time()
            connections_to_remove = []
            
            for connection_id, info in self.connection_info.items():
                if current_time - info["last_activity"] > settings.connection_timeout:
                    connections_to_remove.append(connection_id)
            
            for connection_id in connections_to_remove:
                websocket = self.active_connections.get(connection_id)
                if websocket:
                    try:
                        await websocket.close(
                            code=1000,
                            reason="Connection timeout due to inactivity"
                        )
                    except:
                        pass
                    
                    self.disconnect(websocket)
                    
                    logger.info(
                        "Inactive connection removed",
                        connection_id=connection_id,
                        inactive_seconds=current_time - info["last_activity"]
                    )
    
    async def send_message(self, websocket: WebSocket, message: Dict[str, Any]):
        """Отправка сообщения конкретному соединению"""
        connection_id = self.connection_ids.get(websocket)
        
        if not connection_id:
            return False
        
        try:
            await websocket.send_json(message)
            
            # Обновление статистики
            async with self.lock:
                if connection_id in self.connection_info:
                    self.connection_info[connection_id]["message_count_sent"] += 1
                    self.connection_info[connection_id]["last_activity"] = time.time()
                
                self.stats["messages_sent"] += 1
            
            # Сохранение в историю
            self.message_history[connection_id].append({
                "direction": "out",
                "timestamp": time.time(),
                "message": message
            })
            
            return True
            
        except Exception as e:
            logger.error(
                "Failed to send message",
                connection_id=connection_id,
                error=str(e)
            )
            
            async with self.lock:
                self.stats["errors"] += 1
            
            return False
    
    async def send_to_connection(self, connection_id: str, message: Dict[str, Any]):
        """Отправка сообщения по ID соединения"""
        async with self.lock:
            websocket = self.active_connections.get(connection_id)
            
        if not websocket:
            return False
        
        return await self.send_message(websocket, message)
    
    async def broadcast(self, message: Dict[str, Any], exclude: Optional[List[str]] = None):
        """Широковещательная рассылка всем соединениям"""
        exclude = exclude or []
        
        async with self.lock:
            connections = [
                (conn_id, websocket)
                for conn_id, websocket in self.active_connections.items()
                if conn_id not in exclude
            ]
        
        results = []
        for connection_id, websocket in connections:
            success = await self.send_message(websocket, message)
            results.append((connection_id, success))
        
        return results
    
    async def broadcast_to_topic(self, topic: str, message: Dict[str, Any]):
        """Рассылка сообщения подписчикам темы"""
        async with self.lock:
            subscribers = list(self.subscriptions.get(topic, set()))
        
        results = []
        for connection_id in subscribers:
            success = await self.send_to_connection(connection_id, message)
            results.append((connection_id, success))
        
        return results
    
    async def subscribe(self, websocket: WebSocket, topic: str):
        """Подписка соединения на тему"""
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            
            if not connection_id:
                return False
            
            # Добавление подписки
            self.subscriptions[topic].add(connection_id)
            
            if connection_id in self.connection_info:
                self.connection_info[connection_id]["subscriptions"].add(topic)
            
            logger.debug(
                "Subscription added",
                connection_id=connection_id,
                topic=topic,
                total_subscribers=len(self.subscriptions[topic])
            )
            
            return True
    
    async def unsubscribe(self, websocket: WebSocket, topic: str):
        """Отписка соединения от темы"""
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            
            if not connection_id:
                return False
            
            # Удаление подписки
            if connection_id in self.subscriptions[topic]:
                self.subscriptions[topic].remove(connection_id)
            
            if connection_id in self.connection_info:
                self.connection_info[connection_id]["subscriptions"].discard(topic)
            
            logger.debug(
                "Subscription removed",
                connection_id=connection_id,
                topic=topic,
                total_subscribers=len(self.subscriptions[topic])
            )
            
            return True
    
    def get_connection_id(self, websocket: WebSocket) -> Optional[str]:
        """Получение ID соединения"""
        return self.connection_ids.get(websocket)
    
    def get_connection_info(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Получение информации о соединении"""
        return self.connection_info.get(connection_id)
    
    def active_connections_count(self) -> int:
        """Количество активных соединений"""
        return len(self.active_connections)
    
    def total_connections_accepted(self) -> int:
        """Общее количество принятых соединений"""
        return self.stats["connections_accepted"]
    
    def total_connections_rejected(self) -> int:
        """Общее количество отклоненных соединений"""
        return self.stats["connections_rejected"]
    
    async def get_connection_stats(self) -> Dict[str, Any]:
        """Получение статистики соединений"""
        async with self.lock:
            now = time.time()
            
            # Расчет времени жизни соединений
            connection_ages = []
            for info in self.connection_info.values():
                age = now - info["connected_at"]
                connection_ages.append(age)
            
            # Группировка по IP
            ip_distribution = {
                ip: len(connections)
                for ip, connections in self.connections_by_ip.items()
            }
            
            # Подсчет подписок
            topic_stats = []
            for topic, subscribers in self.subscriptions.items():
                if subscribers:
                    topic_stats.append({
                        "topic": topic,
                        "subscribers": len(subscribers)
                    })
            
            return {
                "active_connections": len(self.active_connections),
                "unique_ips": len(self.connections_by_ip),
                "connection_stats": {
                    "avg_age_seconds": sum(connection_ages) / len(connection_ages) if connection_ages else 0,
                    "max_age_seconds": max(connection_ages) if connection_ages else 0,
                    "min_age_seconds": min(connection_ages) if connection_ages else 0
                },
                "ip_distribution": ip_distribution,
                "topic_stats": topic_stats,
                "message_stats": {
                    "sent": self.stats["messages_sent"],
                    "received": self.stats["messages_received"],
                    "errors": self.stats["errors"]
                }
            }
    
    def get_ip_limits(self) -> Dict[str, Any]:
        """Получение информации об ограничениях по IP"""
        return {
            "max_connections_per_ip": settings.max_connections_per_ip,
            "current_distribution": {
                ip: len(connections)
                for ip, connections in self.connections_by_ip.items()
            }
        }
    
    async def get_message_stats(self) -> Dict[str, Any]:
        """Получение статистики сообщений"""
        async with self.lock:
            total_messages_received = sum(
                info.get("message_count_received", 0)
                for info in self.connection_info.values()
            )
            
            total_messages_sent = sum(
                info.get("message_count_sent", 0)
                for info in self.connection_info.values()
            )
            
            return {
                "total_received": total_messages_received,
                "total_sent": total_messages_sent,
                "server_stats": self.stats
            }
    
    async def update_activity(self, websocket: WebSocket, received: bool = False):
        """Обновление времени последней активности"""
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            
            if connection_id and connection_id in self.connection_info:
                self.connection_info[connection_id]["last_activity"] = time.time()
                
                if received:
                    self.connection_info[connection_id]["message_count_received"] += 1
                    self.stats["messages_received"] += 1
    
    def get_message_history(self, connection_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Получение истории сообщений соединения"""
        history = self.message_history.get(connection_id, deque())
        return list(history)[-limit:]