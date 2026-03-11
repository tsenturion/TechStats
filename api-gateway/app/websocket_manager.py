import asyncio
import time
import uuid
from typing import Dict

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self):
        self._connections: Dict[str, WebSocket] = {}
        self._id_by_socket: Dict[WebSocket, str] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> str:
        connection_id = f"gw_{uuid.uuid4().hex[:12]}"
        async with self._lock:
            self._connections[connection_id] = websocket
            self._id_by_socket[websocket] = connection_id
        return connection_id

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            connection_id = self._id_by_socket.pop(websocket, None)
            if connection_id:
                self._connections.pop(connection_id, None)

    def active_connections_count(self) -> int:
        return len(self._connections)

    async def broadcast(self, message: dict) -> None:
        async with self._lock:
            sockets = list(self._connections.values())
        for websocket in sockets:
            try:
                await websocket.send_json(message)
            except Exception:
                pass

    def snapshot(self) -> dict:
        return {
            "active_connections": len(self._connections),
            "timestamp": time.time(),
        }


websocket_manager = WebSocketManager()

