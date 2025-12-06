# C:\Users\user\Desktop\TechStats\api-gateway\app\routers\websocket.py
import json
import asyncio
from typing import Dict, Any
import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import structlog

from config import settings
from app.websocket_manager import websocket_manager

router = APIRouter()
logger = structlog.get_logger()


@router.websocket("/ws/analyze")
async def websocket_analyze(websocket: WebSocket):
    """
    WebSocket endpoint для анализа с прогрессом в реальном времени
    """
    await websocket.accept()
    
    try:
        # Получение параметров запроса
        data = await websocket.receive_json()
        
        # Валидация
        required_fields = ["vacancy_title", "technology"]
        for field in required_fields:
            if field not in data:
                await websocket.send_json({
                    "error": f"Missing required field: {field}",
                    "stage": "error"
                })
                return
        
        # Подключение к WebSocket сервису
        async with httpx.AsyncClient() as client:
            try:
                # Отправляем запрос в WebSocket сервис
                async with client.stream(
                    "POST",
                    f"{settings.websocket_service_url}/api/v1/ws/proxy",
                    json=data,
                    timeout=30.0
                ) as response:
                    
                    if response.status_code != 200:
                        error_data = await response.json()
                        await websocket.send_json({
                            "error": error_data.get("detail", "WebSocket service error"),
                            "stage": "error"
                        })
                        return
                    
                    # Проксирование сообщений между клиентом и сервисом
                    async for chunk in response.aiter_bytes():
                        try:
                            message = json.loads(chunk.decode())
                            await websocket.send_json(message)
                            
                            # Если анализ завершен, выходим
                            if message.get("stage") == "finished":
                                break
                                
                        except json.JSONDecodeError:
                            logger.warning("Failed to parse WebSocket message", chunk=chunk)
                        
            except httpx.TimeoutException:
                await websocket.send_json({
                    "error": "WebSocket service timeout",
                    "stage": "error"
                })
            except Exception as e:
                logger.error("WebSocket proxy error", error=str(e))
                await websocket.send_json({
                    "error": f"WebSocket error: {str(e)}",
                    "stage": "error"
                })
                
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected by client")
    except json.JSONDecodeError:
        await websocket.send_json({
            "error": "Invalid JSON received",
            "stage": "error"
        })
    except Exception as e:
        logger.error("WebSocket endpoint error", error=str(e))
        try:
            await websocket.send_json({
                "error": f"Server error: {str(e)}",
                "stage": "error"
            })
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


@router.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    """
    WebSocket для передачи метрик в реальном времени
    """
    await websocket.accept()
    
    try:
        # Регистрация подключения
        connection_id = await websocket_manager.connect(websocket)
        
        # Отправка метрик каждые 5 секунд
        while True:
            metrics = {
                "connections": websocket_manager.active_connections_count(),
                "timestamp": asyncio.get_event_loop().time(),
                "services": {
                    "vacancy": await check_service_health(settings.vacancy_service_url),
                    "analyzer": await check_service_health(settings.analyzer_service_url),
                    "cache": await check_service_health(settings.cache_service_url),
                }
            }
            
            await websocket.send_json({
                "type": "metrics",
                "data": metrics
            })
            
            await asyncio.sleep(5)
            
    except WebSocketDisconnect:
        await websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error("Metrics WebSocket error", error=str(e))
        try:
            await websocket.close()
        except:
            pass


async def check_service_health(url: str) -> Dict[str, Any]:
    """Проверка здоровья сервиса"""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{url}/health")
            return {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "response_time": response.elapsed.total_seconds(),
                "status_code": response.status_code
            }
    except Exception as e:
        return {
            "status": "unavailable",
            "error": str(e)
        }