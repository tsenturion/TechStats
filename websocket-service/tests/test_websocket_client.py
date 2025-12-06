# C:\Users\user\Desktop\TechStats\websocket-service\tests\test_websocket_client.py
import asyncio
import json
import websockets
import httpx
from typing import Dict, Any


async def test_websocket_analyze():
    """Тест WebSocket соединения для анализа"""
    uri = "ws://localhost:8004/api/v1/ws/analyze"
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ WebSocket соединение установлено")
            
            # Отправка запроса на анализ
            request = {
                "type": "analyze",
                "vacancy_title": "Python Developer",
                "technology": "Python",
                "exact_search": True,
                "area": 113,
                "max_pages": 2,
                "per_page": 10
            }
            
            await websocket.send(json.dumps(request))
            print("📤 Запрос отправлен:", json.dumps(request, indent=2))
            
            # Получение ответов
            progress_count = 0
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                
                message_type = data.get("type")
                
                if message_type == "progress":
                    progress_count += 1
                    stage = data.get("stage", "")
                    progress = data.get("progress", 0)
                    message = data.get("message", "")
                    
                    print(f"📊 Прогресс [{progress_count}]: {stage} - {progress}% - {message}")
                    
                    if progress >= 100:
                        print("✅ Анализ завершен!")
                        break
                        
                elif message_type == "error":
                    print(f"❌ Ошибка: {data.get('message')}")
                    break
                    
                elif message_type == "completed":
                    print(f"🎉 Анализ завершен с результатом")
                    print(json.dumps(data.get("metadata", {}).get("result", {}), indent=2))
                    break
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")


async def test_http_endpoints():
    """Тест HTTP endpoints WebSocket сервиса"""
    base_url = "http://localhost:8004"
    
    async with httpx.AsyncClient() as client:
        # Проверка здоровья
        response = await client.get(f"{base_url}/api/v1/health")
        print(f"✅ Health check: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
        
        # Получение статистики соединений
        response = await client.get(f"{base_url}/api/v1/ws/connections")
        print(f"\n✅ Connection stats: {response.status_code}")
        stats = response.json()
        print(f"Active connections: {stats.get('active_count', 0)}")
        
        # Получение активных сессий
        response = await client.get(f"{base_url}/api/v1/ws/sessions?limit=5")
        print(f"\n✅ Active sessions: {response.status_code}")
        sessions = response.json()
        print(f"Total sessions: {sessions.get('total', 0)}")


async def test_admin_endpoints():
    """Тест административных endpoints"""
    base_url = "http://localhost:8004"
    admin_token = "admin_secret_token"  # Должен совпадать с настройками
    
    async with httpx.AsyncClient() as client:
        # Попытка доступа без токена
        response = await client.get(f"{base_url}/api/v1/admin/connections")
        print(f"❌ Access without token (expected 403): {response.status_code}")
        
        # Доступ с токеном
        headers = {"Authorization": f"Bearer {admin_token}"}
        
        response = await client.get(
            f"{base_url}/api/v1/admin/connections",
            headers=headers
        )
        print(f"\n✅ Admin connections with token: {response.status_code}")
        
        # Получение системной информации
        response = await client.get(
            f"{base_url}/api/v1/admin/system/info",
            headers=headers
        )
        print(f"\n✅ System info: {response.status_code}")
        info = response.json()
        print(f"Active connections: {info.get('connections', {}).get('active_count', 0)}")
        print(f"Active sessions: {info.get('sessions', {}).get('active', 0)}")


async def main():
    """Основная функция тестирования"""
    print("🚀 Тестирование WebSocket Service\n")
    
    print("=" * 50)
    print("1. Тестирование HTTP endpoints")
    print("=" * 50)
    await test_http_endpoints()
    
    print("\n" + "=" * 50)
    print("2. Тестирование административных endpoints")
    print("=" * 50)
    await test_admin_endpoints()
    
    print("\n" + "=" * 50)
    print("3. Тестирование WebSocket соединения")
    print("=" * 50)
    print("⚠️  Примечание: WebSocket тест требует запущенного vacancy и analyzer сервисов")
    
    try:
        # Проверяем доступность зависимых сервисов
        async with httpx.AsyncClient() as client:
            vacancy_response = await client.get("http://localhost:8001/api/v1/health", timeout=2)
            analyzer_response = await client.get("http://localhost:8002/api/v1/health", timeout=2)
            
            if vacancy_response.status_code == 200 and analyzer_response.status_code == 200:
                print("✅ Зависимые сервисы доступны, запускаем WebSocket тест...")
                await test_websocket_analyze()
            else:
                print("⚠️  Пропускаем WebSocket тест: зависимые сервисы не доступны")
    except:
        print("⚠️  Пропускаем WebSocket тест: не удалось проверить зависимые сервисы")
    
    print("\n" + "=" * 50)
    print("✅ Все тесты завершены")


if __name__ == "__main__":
    asyncio.run(main())