import asyncio
import time
import uuid
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

import structlog
from fastapi import HTTPException, WebSocket

from config import settings

logger = structlog.get_logger()


class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_info: Dict[str, Dict[str, Any]] = {}
        self.connection_ids: Dict[WebSocket, str] = {}
        self.subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self.connections_by_ip: Dict[str, List[str]] = defaultdict(list)
        self.message_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.stats = {
            "connections_accepted": 0,
            "connections_rejected": 0,
            "messages_sent": 0,
            "messages_received": 0,
            "errors": 0,
        }
        self.lock = asyncio.Lock()

    def generate_connection_id(self) -> str:
        return f"conn_{uuid.uuid4().hex[:16]}"

    async def connect(self, websocket: WebSocket) -> str:
        async with self.lock:
            client_ip = websocket.client.host if websocket.client else "unknown"

            if len(self.connections_by_ip[client_ip]) >= settings.max_connections_per_ip:
                self.stats["connections_rejected"] += 1
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many connections from this IP. Maximum: {settings.max_connections_per_ip}",
                )

            if len(self.active_connections) >= settings.max_total_connections:
                self.stats["connections_rejected"] += 1
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many active connections. Maximum: {settings.max_total_connections}",
                )

            await websocket.accept()
            connection_id = self.generate_connection_id()

            self.active_connections[connection_id] = websocket
            self.connection_ids[websocket] = connection_id
            self.connections_by_ip[client_ip].append(connection_id)
            self.connection_info[connection_id] = {
                "client_ip": client_ip,
                "connected_at": time.time(),
                "last_activity": time.time(),
                "message_count_sent": 0,
                "message_count_received": 0,
                "subscriptions": set(),
                "user_agent": websocket.headers.get("user-agent", ""),
                "path": websocket.url.path,
            }
            self.stats["connections_accepted"] += 1
            return connection_id

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self.lock:
            connection_id = self.connection_ids.pop(websocket, None)
            if not connection_id:
                return

            self.active_connections.pop(connection_id, None)
            info = self.connection_info.pop(connection_id, None)

            if info:
                for topic in list(info.get("subscriptions", set())):
                    self.subscriptions[topic].discard(connection_id)

            for ip in list(self.connections_by_ip.keys()):
                if connection_id in self.connections_by_ip[ip]:
                    self.connections_by_ip[ip].remove(connection_id)
                    if not self.connections_by_ip[ip]:
                        del self.connections_by_ip[ip]
                    break

    async def disconnect_all(self) -> None:
        async with self.lock:
            sockets = list(self.active_connections.values())
        for websocket in sockets:
            try:
                await websocket.close(code=1000, reason="Server shutdown")
            except Exception:
                pass
            await self.disconnect(websocket)

    async def cleanup_inactive_connections(self) -> None:
        async with self.lock:
            now = time.time()
            stale_ids = [
                connection_id
                for connection_id, info in self.connection_info.items()
                if now - float(info.get("last_activity", now)) > settings.connection_timeout
            ]
            stale_sockets = [self.active_connections.get(connection_id) for connection_id in stale_ids]

        for websocket in stale_sockets:
            if websocket is None:
                continue
            try:
                await websocket.close(code=1000, reason="Connection timeout due to inactivity")
            except Exception:
                pass
            await self.disconnect(websocket)

    async def send_message(self, websocket: WebSocket, message: Dict[str, Any]) -> bool:
        connection_id = self.connection_ids.get(websocket)
        if not connection_id:
            return False

        try:
            await websocket.send_json(message)
            async with self.lock:
                if connection_id in self.connection_info:
                    self.connection_info[connection_id]["message_count_sent"] += 1
                    self.connection_info[connection_id]["last_activity"] = time.time()
                self.stats["messages_sent"] += 1
            self.message_history[connection_id].append({"direction": "out", "timestamp": time.time(), "message": message})
            return True
        except Exception as exc:
            logger.error("Failed to send message", connection_id=connection_id, error=str(exc))
            async with self.lock:
                self.stats["errors"] += 1
            return False

    async def send_to_connection(self, connection_id: str, message: Dict[str, Any]) -> bool:
        async with self.lock:
            websocket = self.active_connections.get(connection_id)
        if not websocket:
            return False
        return await self.send_message(websocket, message)

    async def broadcast(self, message: Dict[str, Any], exclude: Optional[List[str]] = None) -> List[Tuple[str, bool]]:
        exclude_ids = set(exclude or [])
        async with self.lock:
            targets = [(cid, ws) for cid, ws in self.active_connections.items() if cid not in exclude_ids]
        results = []
        for connection_id, websocket in targets:
            results.append((connection_id, await self.send_message(websocket, message)))
        return results

    async def broadcast_to_topic(self, topic: str, message: Dict[str, Any]) -> List[Tuple[str, bool]]:
        async with self.lock:
            subscribers = list(self.subscriptions.get(topic, set()))
        results = []
        for connection_id in subscribers:
            results.append((connection_id, await self.send_to_connection(connection_id, message)))
        return results

    async def subscribe(self, websocket: WebSocket, topic: str) -> bool:
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            if not connection_id:
                return False
            self.subscriptions[topic].add(connection_id)
            if connection_id in self.connection_info:
                self.connection_info[connection_id]["subscriptions"].add(topic)
            return True

    async def unsubscribe(self, websocket: WebSocket, topic: str) -> bool:
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            if not connection_id:
                return False
            self.subscriptions[topic].discard(connection_id)
            if connection_id in self.connection_info:
                self.connection_info[connection_id]["subscriptions"].discard(topic)
            return True

    def get_connection_id(self, websocket: WebSocket) -> Optional[str]:
        return self.connection_ids.get(websocket)

    def get_connection_info(self, connection_id: str) -> Optional[Dict[str, Any]]:
        return self.connection_info.get(connection_id)

    def active_connections_count(self) -> int:
        return len(self.active_connections)

    def total_connections_accepted(self) -> int:
        return int(self.stats["connections_accepted"])

    def total_connections_rejected(self) -> int:
        return int(self.stats["connections_rejected"])

    def get_connection_stats(self) -> Dict[str, Any]:
        now = time.time()
        connection_ages = [now - info["connected_at"] for info in self.connection_info.values()]
        topic_stats = [
            {"topic": topic, "subscribers": len(subscribers)}
            for topic, subscribers in self.subscriptions.items()
            if subscribers
        ]
        return {
            "active_connections": len(self.active_connections),
            "unique_ips": len(self.connections_by_ip),
            "connection_stats": {
                "avg_age_seconds": sum(connection_ages) / len(connection_ages) if connection_ages else 0,
                "max_age_seconds": max(connection_ages) if connection_ages else 0,
                "min_age_seconds": min(connection_ages) if connection_ages else 0,
            },
            "ip_distribution": {ip: len(conn_ids) for ip, conn_ids in self.connections_by_ip.items()},
            "topic_stats": topic_stats,
            "message_stats": {
                "sent": self.stats["messages_sent"],
                "received": self.stats["messages_received"],
                "errors": self.stats["errors"],
            },
        }

    def get_ip_limits(self) -> Dict[str, Any]:
        return {
            "max_connections_per_ip": settings.max_connections_per_ip,
            "current_distribution": {ip: len(conn_ids) for ip, conn_ids in self.connections_by_ip.items()},
        }

    def get_message_stats(self) -> Dict[str, Any]:
        total_received = sum(info.get("message_count_received", 0) for info in self.connection_info.values())
        total_sent = sum(info.get("message_count_sent", 0) for info in self.connection_info.values())
        return {
            "total_received": total_received,
            "total_sent": total_sent,
            "server_stats": self.stats.copy(),
        }

    async def update_activity(self, websocket: WebSocket, received: bool = False) -> None:
        async with self.lock:
            connection_id = self.connection_ids.get(websocket)
            if connection_id and connection_id in self.connection_info:
                self.connection_info[connection_id]["last_activity"] = time.time()
                if received:
                    self.connection_info[connection_id]["message_count_received"] += 1
                    self.stats["messages_received"] += 1

    def get_message_history(self, connection_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        history = self.message_history.get(connection_id, deque())
        return list(history)[-limit:]

