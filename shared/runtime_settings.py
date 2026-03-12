import copy
from typing import Any, Dict, Mapping, Optional, Tuple


RUNTIME_SETTINGS_KEY = "techstats:runtime:settings:v1"


SETTINGS_SCHEMA: Dict[str, Dict[str, Any]] = {
    "search_default_area": {
        "type": "int",
        "default": 113,
        "min": 1,
        "max": 1_000_000,
        "description": "Default HH area for search forms",
        "scope": "frontend",
        "runtime": True,
    },
    "search_default_exact": {
        "type": "bool",
        "default": True,
        "description": "Default exact_search value",
        "scope": "frontend",
        "runtime": True,
    },
    "search_default_use_cache": {
        "type": "bool",
        "default": True,
        "description": "Default use_cache value",
        "scope": "frontend",
        "runtime": True,
    },
    "search_default_max_pages": {
        "type": "int",
        "default": 3,
        "min": 1,
        "max": 20,
        "description": "Default max_pages for new analyses",
        "scope": "frontend",
        "runtime": True,
    },
    "search_default_per_page": {
        "type": "int",
        "default": 50,
        "min": 1,
        "max": 100,
        "description": "Default per_page for new analyses",
        "scope": "frontend",
        "runtime": True,
    },
    "search_max_pages_hard_limit": {
        "type": "int",
        "default": 20,
        "min": 1,
        "max": 20,
        "description": "Hard limit for max_pages in requests",
        "scope": "gateway",
        "runtime": True,
    },
    "search_per_page_hard_limit": {
        "type": "int",
        "default": 100,
        "min": 1,
        "max": 100,
        "description": "Hard limit for per_page in requests",
        "scope": "gateway",
        "runtime": True,
    },
    "vacancy_batch_max_ids": {
        "type": "int",
        "default": 100,
        "min": 1,
        "max": 100,
        "description": "Max vacancy IDs per one vacancy-service batch request",
        "scope": "gateway+analyzer-service",
        "runtime": True,
    },
    "gateway_vacancy_request_timeout_sec": {
        "type": "int",
        "default": 10,
        "min": 1,
        "max": 120,
        "description": "Gateway timeout for vacancy-service calls",
        "scope": "gateway",
        "runtime": True,
    },
    "gateway_analyzer_request_timeout_sec": {
        "type": "int",
        "default": 30,
        "min": 1,
        "max": 300,
        "description": "Gateway timeout for analyzer-service calls",
        "scope": "gateway",
        "runtime": True,
    },
    "gateway_vacancy_request_delay_ms": {
        "type": "int",
        "default": 0,
        "min": 0,
        "max": 10_000,
        "description": "Optional delay before vacancy-service request",
        "scope": "gateway",
        "runtime": True,
    },
    "gateway_analyzer_request_delay_ms": {
        "type": "int",
        "default": 0,
        "min": 0,
        "max": 10_000,
        "description": "Optional delay before analyzer-service request",
        "scope": "gateway",
        "runtime": True,
    },
    "analysis_default_use_cache": {
        "type": "bool",
        "default": True,
        "description": "Default use_cache for analyze endpoints",
        "scope": "gateway",
        "runtime": True,
    },
    "analysis_max_pages_hard_limit": {
        "type": "int",
        "default": 20,
        "min": 1,
        "max": 20,
        "description": "Hard max_pages limit for analyzer pipeline",
        "scope": "gateway",
        "runtime": True,
    },
    "analysis_per_page_hard_limit": {
        "type": "int",
        "default": 100,
        "min": 1,
        "max": 100,
        "description": "Hard per_page limit for analyzer pipeline",
        "scope": "gateway",
        "runtime": True,
    },
    "live_progress_update_interval_sec": {
        "type": "float",
        "default": 0.5,
        "min": 0.05,
        "max": 60.0,
        "description": "Live-analysis progress update interval",
        "scope": "websocket-service",
        "runtime": True,
    },
    "live_progress_keepalive_interval_sec": {
        "type": "float",
        "default": 5.0,
        "min": 0.1,
        "max": 300.0,
        "description": "Live-analysis keepalive update interval when no progress changed",
        "scope": "websocket-service",
        "runtime": True,
    },
    "live_progress_batch_size": {
        "type": "int",
        "default": 10,
        "min": 1,
        "max": 500,
        "description": "Live-analysis pseudo-progress batch size",
        "scope": "websocket-service",
        "runtime": True,
    },
    "live_vacancy_request_timeout_sec": {
        "type": "int",
        "default": 30,
        "min": 5,
        "max": 300,
        "description": "Timeout for vacancy-service calls inside live analysis pipeline",
        "scope": "websocket-service",
        "runtime": True,
    },
    "live_analyzer_request_timeout_sec": {
        "type": "int",
        "default": 180,
        "min": 10,
        "max": 900,
        "description": "Per-request timeout for analyzer-service calls inside live analysis pipeline",
        "scope": "websocket-service",
        "runtime": True,
    },
    "live_analyzer_total_timeout_sec": {
        "type": "int",
        "default": 1800,
        "min": 60,
        "max": 7200,
        "description": "Total timeout for live analysis wait loop while polling analyzer async task",
        "scope": "websocket-service",
        "runtime": True,
    },
    "live_max_total_vacancies": {
        "type": "int",
        "default": 2000,
        "min": 10,
        "max": 5000,
        "description": "Hard cap for total vacancies per live analysis",
        "scope": "websocket-service",
        "runtime": True,
    },
    "hh_rate_limit_per_second": {
        "type": "int",
        "default": 7,
        "min": 1,
        "max": 20,
        "description": "Runtime HH requests-per-second limit",
        "scope": "vacancy-service",
        "runtime": True,
    },
    "hh_rate_limit_per_day": {
        "type": "int",
        "default": 50_000,
        "min": 100,
        "max": 200_000,
        "description": "Runtime HH requests-per-day limit",
        "scope": "vacancy-service",
        "runtime": True,
    },
    "analyzer_batch_size": {
        "type": "int",
        "default": 10,
        "min": 1,
        "max": 200,
        "description": "Batch size for analyzer pattern matching",
        "scope": "analyzer-service",
        "runtime": True,
    },
    "analyzer_detail_request_timeout_sec": {
        "type": "int",
        "default": 45,
        "min": 5,
        "max": 300,
        "description": "Timeout for one analyzer-service request to vacancy-service batch endpoint",
        "scope": "analyzer-service",
        "runtime": True,
    },
    "analyzer_detail_retry_attempts": {
        "type": "int",
        "default": 1,
        "min": 1,
        "max": 5,
        "description": "Retry attempts for analyzer-service calls to vacancy-service batch endpoint",
        "scope": "analyzer-service",
        "runtime": True,
    },
    "analyzer_detail_chunk_hard_timeout_sec": {
        "type": "int",
        "default": 120,
        "min": 10,
        "max": 900,
        "description": "Hard timeout for one batch chunk details fetch in analyzer-service",
        "scope": "analyzer-service",
        "runtime": True,
    },
    "auth_access_token_expire_minutes": {
        "type": "int",
        "default": 30,
        "min": 5,
        "max": 1440,
        "description": "Gateway JWT access token expiration",
        "scope": "gateway-auth",
        "runtime": True,
    },
    "auth_refresh_token_expire_minutes": {
        "type": "int",
        "default": 10080,
        "min": 30,
        "max": 43200,
        "description": "Gateway JWT refresh token expiration",
        "scope": "gateway-auth",
        "runtime": True,
    },
}

SETTINGS_DESCRIPTIONS_RU: Dict[str, str] = {
    "search_default_area": "Регион HH по умолчанию для поисковых форм.",
    "search_default_exact": "Значение exact_search по умолчанию.",
    "search_default_use_cache": "Значение use_cache по умолчанию.",
    "search_default_max_pages": "Значение max_pages по умолчанию для новых анализов.",
    "search_default_per_page": "Значение per_page по умолчанию для новых анализов.",
    "search_max_pages_hard_limit": "Жесткий лимит max_pages в запросах.",
    "search_per_page_hard_limit": "Жесткий лимит per_page в запросах.",
    "vacancy_batch_max_ids": "Максимум ID вакансий в одном batch-запросе к vacancy-service.",
    "gateway_vacancy_request_timeout_sec": "Таймаут gateway для вызовов vacancy-service.",
    "gateway_analyzer_request_timeout_sec": "Таймаут gateway для вызовов analyzer-service.",
    "gateway_vacancy_request_delay_ms": "Дополнительная задержка перед запросом в vacancy-service.",
    "gateway_analyzer_request_delay_ms": "Дополнительная задержка перед запросом в analyzer-service.",
    "analysis_default_use_cache": "Значение use_cache по умолчанию для эндпоинтов анализа.",
    "analysis_max_pages_hard_limit": "Жесткий лимит max_pages в пайплайне анализатора.",
    "analysis_per_page_hard_limit": "Жесткий лимит per_page в пайплайне анализатора.",
    "live_progress_update_interval_sec": "Интервал обновления прогресса Live Analysis.",
    "live_progress_keepalive_interval_sec": "Интервал keepalive-обновлений при отсутствии изменений прогресса.",
    "live_progress_batch_size": "Размер batch для pseudo-progress в Live Analysis.",
    "live_vacancy_request_timeout_sec": "Таймаут вызовов vacancy-service внутри Live Analysis.",
    "live_analyzer_request_timeout_sec": "Таймаут одного запроса к analyzer-service внутри Live Analysis.",
    "live_analyzer_total_timeout_sec": "Общий таймаут ожидания async-задачи analyzer в Live Analysis.",
    "live_max_total_vacancies": "Жесткий лимит общего числа вакансий на один Live Analysis.",
    "hh_rate_limit_per_second": "Runtime-лимит запросов к HH в секунду.",
    "hh_rate_limit_per_day": "Runtime-лимит запросов к HH в день.",
    "analyzer_batch_size": "Размер batch для поиска паттернов в analyzer-service.",
    "analyzer_detail_request_timeout_sec": "Таймаут одного запроса analyzer-service к batch endpoint vacancy-service.",
    "analyzer_detail_retry_attempts": "Количество retry для вызовов analyzer-service к batch endpoint vacancy-service.",
    "analyzer_detail_chunk_hard_timeout_sec": "Жесткий таймаут загрузки деталей для одного batch-чанка в analyzer-service.",
    "auth_access_token_expire_minutes": "Срок жизни access JWT токена gateway (в минутах).",
    "auth_refresh_token_expire_minutes": "Срок жизни refresh JWT токена gateway (в минутах).",
}


def runtime_settings_defaults() -> Dict[str, Any]:
    return {key: meta["default"] for key, meta in SETTINGS_SCHEMA.items()}


def runtime_settings_schema() -> Dict[str, Dict[str, Any]]:
    schema = copy.deepcopy(SETTINGS_SCHEMA)
    for key, meta in schema.items():
        meta["description_ru"] = SETTINGS_DESCRIPTIONS_RU.get(key, meta.get("description", ""))
    return schema


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError("value is not a boolean")


def _normalize_value(meta: Dict[str, Any], value: Any) -> Any:
    value_type = meta.get("type")

    if value_type == "bool":
        normalized = _normalize_bool(value)
    elif value_type == "int":
        normalized = int(value)
    elif value_type == "float":
        normalized = float(value)
    elif value_type == "str":
        normalized = str(value)
    else:
        raise ValueError(f"unsupported setting type: {value_type}")

    min_value = meta.get("min")
    if min_value is not None and normalized < min_value:
        raise ValueError(f"value must be >= {min_value}")

    max_value = meta.get("max")
    if max_value is not None and normalized > max_value:
        raise ValueError(f"value must be <= {max_value}")

    allowed_values = meta.get("values")
    if allowed_values is not None and normalized not in allowed_values:
        raise ValueError(f"value must be one of {allowed_values}")

    return normalized


def sanitize_runtime_settings(raw_values: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    sanitized: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    for key, value in raw_values.items():
        meta = SETTINGS_SCHEMA.get(key)
        if meta is None:
            errors[key] = "unknown setting key"
            continue
        try:
            sanitized[key] = _normalize_value(meta, value)
        except Exception as exc:  # noqa: BLE001
            errors[key] = str(exc)

    return sanitized, errors


def build_effective_runtime_settings(overrides: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    effective = runtime_settings_defaults()
    if not overrides:
        return effective

    sanitized, _ = sanitize_runtime_settings(overrides)
    effective.update(sanitized)
    return effective
