import json
import time
import uuid
from typing import Any, Dict, List, Optional

import redis.asyncio as redis
import structlog

from config import settings

logger = structlog.get_logger()


class SessionStore:
    """Session storage for websocket analysis jobs."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.session_ttl = settings.session_ttl_seconds

    async def initialize(self):
        await self.redis.ping()
        logger.info("Session store initialized")

    def generate_session_id(self) -> str:
        return f"session_{uuid.uuid4().hex}"

    @staticmethod
    def _decode_session_id(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    async def _read_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        data = await self.redis.get(f"session:{session_id}")
        if not data:
            return None
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    async def create_session(self, session_data: Dict[str, Any], ttl: Optional[int] = None) -> str:
        session_id = self.generate_session_id()
        now = time.time()
        payload = {
            **session_data,
            "id": session_id,
            "created_at": now,
            "updated_at": now,
            "status": "created",
            "progress": 0.0,
            "stage": "initializing",
        }
        await self.redis.setex(f"session:{session_id}", ttl or self.session_ttl, json.dumps(payload, ensure_ascii=False))
        await self.redis.sadd("sessions:active", session_id)
        return session_id

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        session = await self._read_session(session_id)
        if not session:
            return None

        ttl = await self.redis.ttl(f"session:{session_id}")
        expires = self.session_ttl if ttl is None or ttl <= 0 else int(ttl)
        session["last_accessed"] = time.time()
        session["updated_at"] = time.time()
        await self.redis.setex(f"session:{session_id}", expires, json.dumps(session, ensure_ascii=False))
        return session

    async def update_session(self, session_id: str, updates: Dict[str, Any], extend_ttl: bool = True):
        session = await self._read_session(session_id)
        if not session:
            return False

        session.update(updates)
        session["updated_at"] = time.time()
        ttl = self.session_ttl if extend_ttl else await self.redis.ttl(f"session:{session_id}")
        if ttl is None or ttl <= 0:
            ttl = self.session_ttl

        await self.redis.setex(f"session:{session_id}", int(ttl), json.dumps(session, ensure_ascii=False))
        return True

    async def update_progress(
        self,
        session_id: str,
        progress: float,
        stage: str,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        updates: Dict[str, Any] = {
            "progress": progress,
            "stage": stage,
            "message": message,
            "last_progress_update": time.time(),
        }
        if metadata:
            updates["metadata"] = metadata
        return await self.update_session(session_id, updates)

    async def complete_session(self, session_id: str, result: Dict[str, Any], status: str = "completed"):
        success = await self.update_session(
            session_id,
            {
                "status": status,
                "completed_at": time.time(),
                "result": result,
                "progress": 100.0,
                "stage": "completed",
            },
            extend_ttl=False,
        )
        if success:
            await self.redis.srem("sessions:active", session_id)
            await self.redis.sadd("sessions:completed", session_id)
            await self.redis.expire(f"session:{session_id}", self.session_ttl * 2)
        return success

    async def fail_session(self, session_id: str, error_message: str, error_details: Optional[Dict[str, Any]] = None):
        success = await self.update_session(
            session_id,
            {
                "status": "failed",
                "failed_at": time.time(),
                "error": error_message,
                "error_details": error_details or {},
                "progress": 100.0,
                "stage": "failed",
            },
            extend_ttl=False,
        )
        if success:
            await self.redis.srem("sessions:active", session_id)
            await self.redis.sadd("sessions:failed", session_id)
        return success

    async def delete_session(self, session_id: str):
        await self.redis.delete(f"session:{session_id}")
        await self.redis.srem("sessions:active", session_id)
        await self.redis.srem("sessions:completed", session_id)
        await self.redis.srem("sessions:failed", session_id)
        return True

    async def cleanup_expired_sessions(self) -> int:
        all_ids = []
        all_ids.extend(await self.redis.smembers("sessions:active"))
        all_ids.extend(await self.redis.smembers("sessions:completed"))
        all_ids.extend(await self.redis.smembers("sessions:failed"))

        cleaned = 0
        for raw_id in all_ids:
            session_id = self._decode_session_id(raw_id)
            ttl = await self.redis.ttl(f"session:{session_id}")
            if ttl is not None and ttl <= 0:
                await self.delete_session(session_id)
                cleaned += 1
        return cleaned

    async def get_active_sessions(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        session_ids = list(await self.redis.smembers("sessions:active"))
        session_ids = [self._decode_session_id(item) for item in session_ids][offset : offset + limit]
        result = []
        for session_id in session_ids:
            session = await self.get_session(session_id)
            if session:
                result.append(session)
        return result

    async def get_session_stats(self) -> Dict[str, Any]:
        active_count = await self.redis.scard("sessions:active")
        completed_count = await self.redis.scard("sessions:completed")
        failed_count = await self.redis.scard("sessions:failed")
        active_sessions = await self.get_active_sessions(limit=1000)

        stage_distribution: Dict[str, int] = {}
        for session in active_sessions:
            stage = session.get("stage", "unknown")
            stage_distribution[stage] = stage_distribution.get(stage, 0) + 1

        avg_progress = (
            sum(float(session.get("progress", 0)) for session in active_sessions) / len(active_sessions)
            if active_sessions
            else 0.0
        )
        return {
            "total_sessions": active_count + completed_count + failed_count,
            "active": active_count,
            "completed": completed_count,
            "failed": failed_count,
            "stage_distribution": stage_distribution,
            "average_progress": avg_progress,
            "last_cleanup": time.time(),
        }

    async def search_sessions(self, query: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
        all_ids = []
        all_ids.extend(await self.redis.smembers("sessions:active"))
        all_ids.extend(await self.redis.smembers("sessions:completed"))
        all_ids.extend(await self.redis.smembers("sessions:failed"))

        matched = []
        for raw_id in all_ids[:1000]:
            session_id = self._decode_session_id(raw_id)
            session = await self._read_session(session_id)
            if not session:
                continue

            if "status" in query and session.get("status") != query["status"]:
                continue
            if "stage" in query and session.get("stage") != query["stage"]:
                continue
            if "min_progress" in query and float(session.get("progress", 0)) < float(query["min_progress"]):
                continue
            if "max_progress" in query and float(session.get("progress", 0)) > float(query["max_progress"]):
                continue
            if "created_after" in query and float(session.get("created_at", 0)) < float(query["created_after"]):
                continue
            if "created_before" in query and float(session.get("created_at", 0)) > float(query["created_before"]):
                continue
            if "search_text" in query:
                search_text = str(query["search_text"]).lower()
                if search_text not in json.dumps(session, ensure_ascii=False).lower():
                    continue

            matched.append(session)
            if len(matched) >= limit:
                break
        return matched

