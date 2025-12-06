# C:\Users\user\Desktop\TechStats\websocket-service\app\session_store.py
import json
import time
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import structlog
import redis.asyncio as redis

from config import settings

logger = structlog.get_logger()


class SessionStore:
    """Хранилище сессий анализа в Redis"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.session_ttl = settings.session_ttl_seconds
        
    async def initialize(self):
        """Инициализация хранилища"""
        try:
            await self.redis.ping()
            logger.info("Session store initialized")
        except Exception as e:
            logger.error("Failed to initialize session store", error=str(e))
            raise
    
    def generate_session_id(self) -> str:
        """Генерация уникального ID сессии"""
        return f"session_{uuid.uuid4().hex}"
    
    async def create_session(
        self,
        session_data: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> str:
        """Создание новой сессии"""
        session_id = self.generate_session_id()
        
        # Добавление метаданных
        session_data.update({
            "id": session_id,
            "created_at": time.time(),
            "updated_at": time.time(),
            "status": "created",
            "progress": 0.0,
            "stage": "initializing"
        })
        
        # Сохранение в Redis
        await self.redis.setex(
            f"session:{session_id}",
            ttl or self.session_ttl,
            json.dumps(session_data, ensure_ascii=False)
        )
        
        # Добавление в индекс сессий
        await self.redis.sadd("sessions:active", session_id)
        
        logger.debug("Session created", session_id=session_id)
        
        return session_id
    
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Получение сессии по ID"""
        try:
            data = await self.redis.get(f"session:{session_id}")
            if data:
                session = json.loads(data)
                
                # Обновление времени последнего доступа
                session["last_accessed"] = time.time()
                await self.update_session(session_id, {"last_accessed": session["last_accessed"]})
                
                return session
        except Exception as e:
            logger.error("Failed to get session", session_id=session_id, error=str(e))
        
        return None
    
    async def update_session(
        self,
        session_id: str,
        updates: Dict[str, Any],
        extend_ttl: bool = True
    ):
        """Обновление сессии"""
        try:
            # Получение текущей сессии
            session = await self.get_session(session_id)
            if not session:
                return False
            
            # Применение обновлений
            session.update(updates)
            session["updated_at"] = time.time()
            
            # Сохранение обновленной сессии
            await self.redis.setex(
                f"session:{session_id}",
                self.session_ttl if extend_ttl else await self.redis.ttl(f"session:{session_id}"),
                json.dumps(session, ensure_ascii=False)
            )
            
            logger.debug("Session updated", session_id=session_id, updates=updates.keys())
            
            return True
            
        except Exception as e:
            logger.error("Failed to update session", session_id=session_id, error=str(e))
            return False
    
    async def update_progress(
        self,
        session_id: str,
        progress: float,
        stage: str,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Обновление прогресса сессии"""
        updates = {
            "progress": progress,
            "stage": stage,
            "message": message,
            "last_progress_update": time.time()
        }
        
        if metadata:
            updates["metadata"] = metadata
        
        return await self.update_session(session_id, updates)
    
    async def complete_session(
        self,
        session_id: str,
        result: Dict[str, Any],
        status: str = "completed"
    ):
        """Завершение сессии"""
        updates = {
            "status": status,
            "completed_at": time.time(),
            "result": result,
            "progress": 100.0,
            "stage": "completed"
        }
        
        success = await self.update_session(session_id, updates, extend_ttl=False)
        
        if success:
            # Перемещение из активных в завершенные
            await self.redis.srem("sessions:active", session_id)
            await self.redis.sadd("sessions:completed", session_id)
            
            # Установка TTL для завершенных сессий (сохраняем дольше)
            await self.redis.expire(f"session:{session_id}", self.session_ttl * 2)
            
            logger.info("Session completed", session_id=session_id, status=status)
        
        return success
    
    async def fail_session(
        self,
        session_id: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None
    ):
        """Пометка сессии как неудачной"""
        updates = {
            "status": "failed",
            "failed_at": time.time(),
            "error": error_message,
            "error_details": error_details or {},
            "progress": 100.0,
            "stage": "failed"
        }
        
        success = await self.update_session(session_id, updates, extend_ttl=False)
        
        if success:
            # Перемещение из активных в завершенные
            await self.redis.srem("sessions:active", session_id)
            await self.redis.sadd("sessions:failed", session_id)
            
            logger.error("Session failed", session_id=session_id, error=error_message)
        
        return success
    
    async def delete_session(self, session_id: str):
        """Удаление сессии"""
        try:
            # Удаление из Redis
            await self.redis.delete(f"session:{session_id}")
            
            # Удаление из индексов
            await self.redis.srem("sessions:active", session_id)
            await self.redis.srem("sessions:completed", session_id)
            await self.redis.srem("sessions:failed", session_id)
            
            logger.debug("Session deleted", session_id=session_id)
            
            return True
            
        except Exception as e:
            logger.error("Failed to delete session", session_id=session_id, error=str(e))
            return False
    
    async def cleanup_expired_sessions(self) -> int:
        """Очистка устаревших сессий"""
        try:
            # Получение всех сессий
            active_sessions = await self.redis.smembers("sessions:active")
            completed_sessions = await self.redis.smembers("sessions:completed")
            failed_sessions = await self.redis.smembers("sessions:failed")
            
            all_sessions = list(active_sessions) + list(completed_sessions) + list(failed_sessions)
            
            cleaned_count = 0
            
            for session_id in all_sessions:
                # Проверка TTL
                ttl = await self.redis.ttl(f"session:{session_id}")
                
                if ttl <= 0:
                    # Сессия истекла, удаляем
                    await self.delete_session(session_id)
                    cleaned_count += 1
            
            if cleaned_count > 0:
                logger.debug("Cleaned expired sessions", count=cleaned_count)
            
            return cleaned_count
            
        except Exception as e:
            logger.error("Failed to cleanup expired sessions", error=str(e))
            return 0
    
    async def get_active_sessions(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Получение активных сессий"""
        try:
            session_ids = await self.redis.smembers("sessions:active")
            session_ids = list(session_ids)[offset:offset + limit]
            
            sessions = []
            for session_id in session_ids:
                session = await self.get_session(session_id)
                if session:
                    sessions.append(session)
            
            return sessions
            
        except Exception as e:
            logger.error("Failed to get active sessions", error=str(e))
            return []
    
    async def get_session_stats(self) -> Dict[str, Any]:
        """Получение статистики сессий"""
        try:
            active_count = await self.redis.scard("sessions:active")
            completed_count = await self.redis.scard("sessions:completed")
            failed_count = await self.redis.scard("sessions:failed")
            
            # Получение информации о стадиях
            active_sessions = await self.get_active_sessions(limit=1000)
            
            stages = {}
            for session in active_sessions:
                stage = session.get("stage", "unknown")
                stages[stage] = stages.get(stage, 0) + 1
            
            # Расчет среднего прогресса
            total_progress = sum(s.get("progress", 0) for s in active_sessions)
            avg_progress = total_progress / len(active_sessions) if active_sessions else 0
            
            return {
                "total_sessions": active_count + completed_count + failed_count,
                "active": active_count,
                "completed": completed_count,
                "failed": failed_count,
                "stage_distribution": stages,
                "average_progress": avg_progress,
                "last_cleanup": time.time()
            }
            
        except Exception as e:
            logger.error("Failed to get session stats", error=str(e))
            return {"error": str(e)}
    
    async def search_sessions(
        self,
        query: Dict[str, Any],
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Поиск сессий по критериям"""
        # В production здесь была бы полноценная поисковая система
        # Для MVP ищем по простым критериям
        
        try:
            # Получение всех сессий
            all_session_ids = []
            all_session_ids.extend(await self.redis.smembers("sessions:active"))
            all_session_ids.extend(await self.redis.smembers("sessions:completed"))
            all_session_ids.extend(await self.redis.smembers("sessions:failed"))
            
            matching_sessions = []
            
            for session_id in all_session_ids[:1000]:  # Ограничение для производительности
                session = await self.get_session(session_id)
                if not session:
                    continue
                
                # Проверка критериев
                matches = True
                
                if "status" in query and session.get("status") != query["status"]:
                    matches = False
                
                if "stage" in query and session.get("stage") != query["stage"]:
                    matches = False
                
                if "min_progress" in query and session.get("progress", 0) < query["min_progress"]:
                    matches = False
                
                if "max_progress" in query and session.get("progress", 0) > query["max_progress"]:
                    matches = False
                
                if "created_after" in query and session.get("created_at", 0) < query["created_after"]:
                    matches = False
                
                if "created_before" in query and session.get("created_at", 0) > query["created_before"]:
                    matches = False
                
                # Поиск по тексту
                if "search_text" in query:
                    search_text = query["search_text"].lower()
                    session_text = json.dumps(session).lower()
                    
                    if search_text not in session_text:
                        matches = False
                
                if matches:
                    matching_sessions.append(session)
                    
                    if len(matching_sessions) >= limit:
                        break
            
            return matching_sessions
            
        except Exception as e:
            logger.error("Failed to search sessions", error=str(e))
            return []