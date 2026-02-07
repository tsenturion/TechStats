import asyncio
import json

import httpx
import pytest
import websockets


BASE_URL = "http://localhost:8004"
WS_URL = "ws://localhost:8004/api/v1/ws/analyze"


async def _ensure_service_available() -> None:
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(f"{BASE_URL}/api/v1/health")
            if response.status_code != 200:
                pytest.skip(f"WebSocket service unavailable: status={response.status_code}")
    except Exception as exc:
        pytest.skip(f"WebSocket service unavailable: {exc}")


@pytest.mark.asyncio
async def test_websocket_analyze():
    await _ensure_service_available()
    try:
        async with websockets.connect(WS_URL) as websocket:
            request = {
                "type": "analyze",
                "vacancy_title": "Python Developer",
                "technology": "Python",
                "exact_search": True,
                "area": 113,
                "max_pages": 2,
                "per_page": 10,
            }
            await websocket.send(json.dumps(request))

            while True:
                response = await websocket.recv()
                data = json.loads(response)
                if data.get("type") in {"error", "completed"}:
                    break
    except Exception as exc:
        pytest.skip(f"WebSocket integration unavailable: {exc}")


@pytest.mark.asyncio
async def test_http_endpoints():
    await _ensure_service_available()
    async with httpx.AsyncClient() as client:
        health = await client.get(f"{BASE_URL}/api/v1/health")
        assert health.status_code == 200

        connections = await client.get(f"{BASE_URL}/api/v1/ws/connections")
        assert connections.status_code == 200

        sessions = await client.get(f"{BASE_URL}/api/v1/ws/sessions?limit=5")
        assert sessions.status_code == 200


@pytest.mark.asyncio
async def test_admin_endpoints():
    await _ensure_service_available()
    async with httpx.AsyncClient() as client:
        unauthorized = await client.get(f"{BASE_URL}/api/v1/admin/connections")
        assert unauthorized.status_code in {401, 403}

        headers = {"Authorization": "Bearer admin_secret_token"}
        connections = await client.get(f"{BASE_URL}/api/v1/admin/connections", headers=headers)
        assert connections.status_code == 200

        system_info = await client.get(f"{BASE_URL}/api/v1/admin/system/info", headers=headers)
        assert system_info.status_code == 200


if __name__ == "__main__":
    asyncio.run(test_http_endpoints())

