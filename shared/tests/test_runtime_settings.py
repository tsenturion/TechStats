from shared.runtime_settings import (
    SETTINGS_SCHEMA,
    build_effective_runtime_settings,
    runtime_settings_defaults,
    runtime_settings_schema,
    sanitize_runtime_settings,
)


def test_runtime_settings_defaults_have_all_schema_keys():
    defaults = runtime_settings_defaults()
    assert set(defaults.keys()) == set(SETTINGS_SCHEMA.keys())


def test_runtime_settings_schema_returns_deep_copy():
    schema_copy = runtime_settings_schema()
    assert schema_copy == SETTINGS_SCHEMA
    schema_copy["search_default_area"]["default"] = 999
    assert SETTINGS_SCHEMA["search_default_area"]["default"] != 999


def test_sanitize_runtime_settings_normalizes_valid_values():
    sanitized, errors = sanitize_runtime_settings(
        {
            "search_default_area": "200",
            "search_default_exact": "false",
            "live_progress_update_interval_sec": "1.5",
            "vacancy_batch_max_ids": 77,
        }
    )

    assert errors == {}
    assert sanitized["search_default_area"] == 200
    assert sanitized["search_default_exact"] is False
    assert sanitized["live_progress_update_interval_sec"] == 1.5
    assert sanitized["vacancy_batch_max_ids"] == 77


def test_sanitize_runtime_settings_rejects_unknown_and_invalid_values():
    _, errors = sanitize_runtime_settings(
        {
            "unknown_key": 1,
            "search_default_area": 0,
            "search_default_exact": "maybe",
        }
    )

    assert "unknown_key" in errors
    assert "search_default_area" in errors
    assert "search_default_exact" in errors


def test_build_effective_runtime_settings_applies_only_valid_overrides():
    effective = build_effective_runtime_settings(
        {
            "search_default_area": 500,
            "search_default_exact": False,
            "unknown_setting": "ignored",
            "search_default_max_pages": 999,  # invalid by max boundary
        }
    )

    assert effective["search_default_area"] == 500
    assert effective["search_default_exact"] is False
    assert effective["search_default_max_pages"] == SETTINGS_SCHEMA["search_default_max_pages"]["default"]
    assert "unknown_setting" not in effective
