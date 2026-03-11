from pathlib import Path
import sys

import pytest

SERVICE_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SERVICE_ROOT.parent
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if "app" in sys.modules and not hasattr(sys.modules["app"], "__path__"):
    sys.modules.pop("app", None)

from app import runtime_config
from shared.runtime_settings import SETTINGS_SCHEMA


@pytest.mark.asyncio
async def test_get_runtime_settings_raw_returns_empty_for_non_dict(monkeypatch):
    async def fake_get(_key):
        return "not-a-dict"

    monkeypatch.setattr(runtime_config.cache_manager, "get", fake_get)
    raw = await runtime_config.get_runtime_settings_raw()
    assert raw == {}


@pytest.mark.asyncio
async def test_get_runtime_settings_effective_merges_raw_values(monkeypatch):
    async def fake_get_raw():
        return {"search_default_per_page": 25}

    monkeypatch.setattr(runtime_config, "get_runtime_settings_raw", fake_get_raw)

    effective = await runtime_config.get_runtime_settings_effective()
    assert effective["search_default_per_page"] == 25
    assert effective["search_default_area"] == SETTINGS_SCHEMA["search_default_area"]["default"]


@pytest.mark.asyncio
async def test_update_runtime_settings_validation_error():
    with pytest.raises(ValueError):
        await runtime_config.update_runtime_settings({"unknown_key": 1})


@pytest.mark.asyncio
async def test_update_runtime_settings_persists_updates(monkeypatch):
    persisted = {}

    async def fake_get_raw():
        return {"search_default_area": 113}

    async def fake_save(values):
        persisted.update(values)

    monkeypatch.setattr(runtime_config, "get_runtime_settings_raw", fake_get_raw)
    monkeypatch.setattr(runtime_config, "save_runtime_settings_raw", fake_save)

    effective = await runtime_config.update_runtime_settings({"search_default_area": 225})

    assert effective["search_default_area"] == 225
    assert persisted["search_default_area"] == 225


@pytest.mark.asyncio
async def test_reset_runtime_settings_requires_redis():
    original = runtime_config.cache_manager.redis_client
    runtime_config.cache_manager.redis_client = None
    try:
        with pytest.raises(RuntimeError):
            await runtime_config.reset_runtime_settings()
    finally:
        runtime_config.cache_manager.redis_client = original


@pytest.mark.asyncio
async def test_reset_runtime_settings_with_fake_redis(monkeypatch):
    class DummyRedis:
        def __init__(self):
            self.deleted_keys = []

        async def delete(self, key):
            self.deleted_keys.append(key)
            return 1

    dummy = DummyRedis()
    monkeypatch.setattr(runtime_config.cache_manager, "redis_client", dummy)

    effective = await runtime_config.reset_runtime_settings()
    assert dummy.deleted_keys == ["techstats:runtime:settings:v1"]
    assert effective["search_default_area"] == SETTINGS_SCHEMA["search_default_area"]["default"]
