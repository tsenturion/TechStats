import asyncio

import pytest

import app.analyzer as analyzer_module
from app.analyzer import TextAnalyzer


@pytest.mark.asyncio
async def test_download_nltk_data_skips_download_when_resources_exist(monkeypatch):
    analyzer = TextAnalyzer()

    calls = {"find": 0}

    def fake_find(_resource):
        calls["find"] += 1
        return True

    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("wait_for should not be called when NLTK data exists")

    monkeypatch.setattr(analyzer_module.nltk.data, "find", fake_find)
    monkeypatch.setattr(analyzer_module.asyncio, "wait_for", fail_if_called)

    await analyzer._download_nltk_data()

    assert analyzer.nltk_downloaded is True
    assert calls["find"] >= 2


@pytest.mark.asyncio
async def test_download_nltk_data_timeout_falls_back_without_error(monkeypatch):
    analyzer = TextAnalyzer()

    def fake_find(_resource):
        raise LookupError

    def fake_to_thread(*_args, **_kwargs):
        return object()

    async def fake_wait_for(_awaitable, timeout):  # noqa: ARG001
        raise asyncio.TimeoutError()

    monkeypatch.setattr(analyzer_module.nltk.data, "find", fake_find)
    monkeypatch.setattr(analyzer_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(analyzer_module.asyncio, "wait_for", fake_wait_for)

    await analyzer._download_nltk_data()

    assert analyzer.nltk_downloaded is True
