import json
import os
import time

import pytest
import requests
import websockets


GATEWAY_URL = os.getenv("TECHSTATS_GATEWAY_URL", "http://localhost:8000")
API_BASE = f"{GATEWAY_URL}/api/v1"
SESSION = requests.Session()
SESSION.trust_env = False
pytestmark = pytest.mark.integration


def _is_gateway_available() -> bool:
    try:
        response = SESSION.get(f"{API_BASE}/health", timeout=3)
        return response.status_code == 200
    except Exception:
        return False


def _login(username: str, password: str):
    response = SESSION.post(
        f"{API_BASE}/auth/login",
        json={"username": username, "password": password},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["access_token"]


@pytest.fixture(scope="module")
def gateway_ready():
    if not _is_gateway_available():
        pytest.skip("Integration: api-gateway is unavailable")
    return True


def test_public_endpoints_are_available(gateway_ready):
    health = SESSION.get(f"{API_BASE}/health", timeout=5)
    assert health.status_code == 200

    runtime_public = SESSION.get(f"{API_BASE}/runtime-settings/public", timeout=5)
    assert runtime_public.status_code == 200
    payload = runtime_public.json()
    assert "settings" in payload
    assert "schema" in payload


def test_guest_cannot_access_protected_search(gateway_ready):
    response = SESSION.get(
        f"{API_BASE}/vacancies/search",
        params={"query": "python", "area": 113, "page": 0, "per_page": 5, "exact_search": True},
        timeout=10,
    )
    assert response.status_code == 401


def test_guest_can_register_and_get_user_token(gateway_ready):
    username = f"integration_user_{int(time.time())}"
    response = SESSION.post(
        f"{API_BASE}/auth/register",
        json={"username": username, "password": "StrongPass123"},
        timeout=10,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["username"] == username
    assert payload["role"] == "user"
    assert "access_token" in payload


def test_user_and_admin_role_boundaries(gateway_ready):
    user_token = _login("user", "user")
    admin_token = _login("admin", "admin")

    user_runtime = SESSION.get(
        f"{API_BASE}/admin/runtime-settings",
        headers={"Authorization": f"Bearer {user_token}"},
        timeout=10,
    )
    assert user_runtime.status_code == 403

    admin_runtime = SESSION.get(
        f"{API_BASE}/admin/runtime-settings",
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=10,
    )
    assert admin_runtime.status_code == 200
    assert "settings" in admin_runtime.json()


def test_admin_can_update_and_reset_runtime_settings(gateway_ready):
    admin_token = _login("admin", "admin")
    headers = {"Authorization": f"Bearer {admin_token}"}

    update = SESSION.put(
        f"{API_BASE}/admin/runtime-settings",
        headers=headers,
        json={"updates": {"search_per_page_hard_limit": 3}},
        timeout=10,
    )
    assert update.status_code == 200
    assert update.json()["settings"]["search_per_page_hard_limit"] == 3

    effective = SESSION.get(f"{API_BASE}/runtime-settings", headers=headers, timeout=10)
    assert effective.status_code == 200
    assert effective.json()["settings"]["search_per_page_hard_limit"] == 3

    reset = SESSION.post(f"{API_BASE}/admin/runtime-settings/reset", headers=headers, timeout=10)
    assert reset.status_code == 200
    assert reset.json()["settings"]["search_per_page_hard_limit"] == 100


@pytest.mark.asyncio
async def test_websocket_analyze_requires_auth(gateway_ready):
    ws_url = GATEWAY_URL.replace("http://", "ws://").replace("https://", "wss://") + "/api/v1/ws/analyze"

    async with websockets.connect(ws_url) as ws:
        frame = await ws.recv()
        payload = json.loads(frame)
        assert payload["type"] == "error"
        assert "Authentication required" in payload["message"]


@pytest.mark.asyncio
async def test_websocket_analyze_accepts_authenticated_user(gateway_ready):
    token = _login("user", "user")
    ws_url = (
        GATEWAY_URL.replace("http://", "ws://").replace("https://", "wss://")
        + f"/api/v1/ws/analyze?access_token={token}"
    )

    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({}))
        frame = await ws.recv()
        payload = json.loads(frame)
        assert payload["type"] == "error"
        assert "Missing required field" in payload["message"]
