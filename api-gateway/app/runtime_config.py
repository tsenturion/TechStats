import json
from typing import Any, Dict, Mapping

from dynaconf import Dynaconf

from app.cache import cache_manager
from shared.runtime_settings import (
    RUNTIME_SETTINGS_KEY,
    build_effective_runtime_settings,
    runtime_settings_schema,
    sanitize_runtime_settings,
)


def _build_dynaconf_runtime(effective_values: Mapping[str, Any]) -> Dynaconf:
    runtime = Dynaconf(settings_files=[], environments=False, envvar_prefix="TECHSTATS_RUNTIME")
    for key, value in effective_values.items():
        runtime.set(key, value)
    return runtime


async def get_runtime_settings_raw() -> Dict[str, Any]:
    raw = await cache_manager.get(RUNTIME_SETTINGS_KEY)
    if isinstance(raw, dict):
        return raw
    return {}


async def get_runtime_settings_effective() -> Dict[str, Any]:
    raw = await get_runtime_settings_raw()
    effective = build_effective_runtime_settings(raw)
    runtime = _build_dynaconf_runtime(effective)
    schema = runtime_settings_schema()
    return {key: runtime.get(key) for key in schema.keys()}


async def save_runtime_settings_raw(values: Mapping[str, Any]) -> None:
    if not cache_manager.redis_client:
        raise RuntimeError("Redis is not initialized")
    await cache_manager.redis_client.set(RUNTIME_SETTINGS_KEY, json.dumps(values, ensure_ascii=False))


async def update_runtime_settings(values: Mapping[str, Any]) -> Dict[str, Any]:
    current = await get_runtime_settings_raw()
    sanitized, errors = sanitize_runtime_settings(values)
    if errors:
        raise ValueError(errors)
    current.update(sanitized)
    await save_runtime_settings_raw(current)
    effective = build_effective_runtime_settings(current)
    runtime = _build_dynaconf_runtime(effective)
    schema = runtime_settings_schema()
    return {key: runtime.get(key) for key in schema.keys()}


async def reset_runtime_settings() -> Dict[str, Any]:
    if not cache_manager.redis_client:
        raise RuntimeError("Redis is not initialized")
    await cache_manager.redis_client.delete(RUNTIME_SETTINGS_KEY)
    effective = build_effective_runtime_settings({})
    runtime = _build_dynaconf_runtime(effective)
    schema = runtime_settings_schema()
    return {key: runtime.get(key) for key in schema.keys()}


def get_runtime_settings_schema() -> Dict[str, Dict[str, Any]]:
    return runtime_settings_schema()
