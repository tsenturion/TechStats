from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient
import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if "app" in sys.modules and not hasattr(sys.modules["app"], "__path__"):
    sys.modules.pop("app", None)

from app.middleware import AuthenticationMiddleware
import app.routers.auth as auth_module
import app.routers.runtime_settings as runtime_settings_module
from app.routers.auth import REGISTERED_USERS_KEY, router as auth_router
from app.routers.runtime_settings import router as runtime_settings_router
from app.security import UserRole, create_access_token


def create_test_app():
    app = FastAPI()
    app.add_middleware(AuthenticationMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(auth_router, prefix="/api/v1")
    app.include_router(runtime_settings_router, prefix="/api/v1")
    return app


@pytest.fixture
def client():
    return TestClient(create_test_app())


def _auth_header(role: UserRole, username: str):
    token = create_access_token(username=username, role=role, expires_minutes=60)
    return {"Authorization": f"Bearer {token}"}


def test_login_success_for_user_and_admin(client):
    user_response = client.post("/api/v1/auth/login", json={"username": "user", "password": "user"})
    assert user_response.status_code == 200
    assert user_response.json()["role"] == "user"
    assert "access_token" in user_response.json()

    admin_response = client.post("/api/v1/auth/login", json={"username": "admin", "password": "admin"})
    assert admin_response.status_code == 200
    assert admin_response.json()["role"] == "admin"
    assert "access_token" in admin_response.json()


def test_register_success_and_login_for_registered_user(client, monkeypatch):
    state = {}

    async def fake_get(key):
        return state.get(key)

    async def fake_set(key, value, ttl=300):
        state[key] = value
        return True

    monkeypatch.setattr(auth_module.cache_manager, "get", fake_get)
    monkeypatch.setattr(auth_module.cache_manager, "set", fake_set)

    register_response = client.post(
        "/api/v1/auth/register",
        json={"username": "new_user_01", "password": "StrongPass123"},
    )
    assert register_response.status_code == 200
    assert register_response.json()["role"] == "user"
    assert register_response.json()["username"] == "new_user_01"
    assert register_response.json()["registered"] is True
    assert "access_token" in register_response.json()
    assert REGISTERED_USERS_KEY in state
    assert "new_user_01" in state[REGISTERED_USERS_KEY]

    login_response = client.post(
        "/api/v1/auth/login",
        json={"username": "new_user_01", "password": "StrongPass123"},
    )
    assert login_response.status_code == 200
    assert login_response.json()["role"] == "user"
    assert login_response.json()["username"] == "new_user_01"
    assert "access_token" in login_response.json()


def test_register_rejects_reserved_and_duplicate_username(client, monkeypatch):
    state = {}

    async def fake_get(key):
        return state.get(key)

    async def fake_set(key, value, ttl=300):
        state[key] = value
        return True

    monkeypatch.setattr(auth_module.cache_manager, "get", fake_get)
    monkeypatch.setattr(auth_module.cache_manager, "set", fake_set)

    reserved = client.post(
        "/api/v1/auth/register",
        json={"username": "admin", "password": "StrongPass123"},
    )
    assert reserved.status_code == 409

    first = client.post(
        "/api/v1/auth/register",
        json={"username": "dup_user", "password": "StrongPass123"},
    )
    assert first.status_code == 200

    duplicate = client.post(
        "/api/v1/auth/register",
        json={"username": "dup_user", "password": "StrongPass123"},
    )
    assert duplicate.status_code == 409


def test_login_rejects_invalid_credentials(client):
    response = client.post("/api/v1/auth/login", json={"username": "wrong", "password": "creds"})
    assert response.status_code == 401


def test_me_requires_authentication(client):
    unauthorized = client.get("/api/v1/auth/me")
    assert unauthorized.status_code == 401

    authorized = client.get("/api/v1/auth/me", headers=_auth_header(UserRole.user, "john"))
    assert authorized.status_code == 200
    assert authorized.json()["username"] == "john"
    assert authorized.json()["role"] == "user"


def test_runtime_public_available_without_auth(client):
    response = client.get("/api/v1/runtime-settings/public")
    assert response.status_code == 200
    payload = response.json()
    assert "settings" in payload
    assert "schema" in payload
    assert "search_default_area" in payload["settings"]
    assert payload["schema"]["analysis_default_use_cache"]["description_ru"].startswith("Значение use_cache")


def test_runtime_settings_requires_user_or_admin(client):
    unauthorized = client.get("/api/v1/runtime-settings")
    assert unauthorized.status_code == 401

    authorized = client.get("/api/v1/runtime-settings", headers=_auth_header(UserRole.user, "john"))
    assert authorized.status_code == 200
    assert "settings" in authorized.json()


def test_admin_runtime_settings_unauthorized_includes_cors_headers(client):
    response = client.get("/api/v1/admin/runtime-settings", headers={"Origin": "http://localhost:8088"})
    assert response.status_code == 401
    assert response.headers.get("access-control-allow-origin") in {"*", "http://localhost:8088"}


def test_admin_runtime_settings_role_enforcement(client):
    forbidden = client.get("/api/v1/admin/runtime-settings", headers=_auth_header(UserRole.user, "john"))
    assert forbidden.status_code == 403

    allowed = client.get("/api/v1/admin/runtime-settings", headers=_auth_header(UserRole.admin, "root"))
    assert allowed.status_code == 200
    assert "settings" in allowed.json()
    assert "schema" in allowed.json()
    assert allowed.json()["updated_by"] == "root"


def test_admin_runtime_settings_update_validation_error(client, monkeypatch):
    async def fake_update_runtime_settings(_updates):
        raise ValueError({"search_default_area": "value must be >= 1"})

    monkeypatch.setattr(runtime_settings_module, "update_runtime_settings", fake_update_runtime_settings)

    response = client.put(
        "/api/v1/admin/runtime-settings",
        headers=_auth_header(UserRole.admin, "root"),
        json={"updates": {"search_default_area": 0}},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "validation_errors" in detail


def test_admin_runtime_settings_update_success(client, monkeypatch):
    async def fake_update_runtime_settings(_updates):
        return {"search_default_area": 225}

    monkeypatch.setattr(runtime_settings_module, "update_runtime_settings", fake_update_runtime_settings)

    response = client.put(
        "/api/v1/admin/runtime-settings",
        headers=_auth_header(UserRole.admin, "root"),
        json={"updates": {"search_default_area": 225}},
    )
    assert response.status_code == 200
    assert response.json()["success"] is True
    assert response.json()["settings"]["search_default_area"] == 225


def test_admin_runtime_settings_reset_handles_runtime_error(client, monkeypatch):
    async def fake_reset_runtime_settings():
        raise RuntimeError("Redis is not initialized")

    monkeypatch.setattr(runtime_settings_module, "reset_runtime_settings", fake_reset_runtime_settings)

    response = client.post("/api/v1/admin/runtime-settings/reset", headers=_auth_header(UserRole.admin, "root"))
    assert response.status_code == 503
