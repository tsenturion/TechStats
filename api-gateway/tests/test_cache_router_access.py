from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if "app" in sys.modules and not hasattr(sys.modules["app"], "__path__"):
    sys.modules.pop("app", None)

from app.middleware import AuthenticationMiddleware
import app.routers.cache as cache_module
from app.routers.cache import router as cache_router
from app.security import UserRole, create_access_token


def _auth_header(role: UserRole):
    token = create_access_token(username=f"{role.value}-user", role=role, expires_minutes=30)
    return {"Authorization": f"Bearer {token}"}


def create_test_app():
    app = FastAPI()
    app.add_middleware(AuthenticationMiddleware)
    app.include_router(cache_router, prefix="/api/v1")
    return app


def test_cache_stats_public(monkeypatch):
    async def fake_stats():
        return {"connected": True, "hits": 10}

    monkeypatch.setattr(cache_module.cache_manager, "get_stats", fake_stats)
    client = TestClient(create_test_app())
    response = client.get("/api/v1/cache/stats")
    assert response.status_code == 200
    assert response.json()["connected"] is True


def test_cache_clear_requires_admin(monkeypatch):
    async def fake_clear(pattern="gateway:*"):
        return 3

    monkeypatch.setattr(cache_module.cache_manager, "clear", fake_clear)
    client = TestClient(create_test_app())

    guest = client.delete("/api/v1/cache/clear")
    assert guest.status_code == 401

    user = client.delete("/api/v1/cache/clear", headers=_auth_header(UserRole.user))
    assert user.status_code == 403

    admin = client.delete("/api/v1/cache/clear", headers=_auth_header(UserRole.admin))
    assert admin.status_code == 200
    assert admin.json()["cleared"] == 3
