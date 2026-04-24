import re

import pytest

from app.analyzer import PatternMatcher


class _UnknownTechPatternsLoader:
    def get_pattern(self, technology):  # noqa: ARG002
        return None

    def get_compiled_pattern(self, technology):  # noqa: ARG002
        return None

    def _get_tech_id(self, technology):  # noqa: ARG002
        return None

    def extract_candidate_technologies(self, text):  # noqa: ARG002
        return set()


class _KnownCSharpPatternsLoader:
    def get_pattern(self, technology):  # noqa: ARG002
        return {"name": "C#", "category": "programming_language"}

    def get_compiled_pattern(self, technology):  # noqa: ARG002
        return re.compile(r"(?<!\w)c#(?!\w)", re.IGNORECASE | re.UNICODE)

    def _get_tech_id(self, technology):  # noqa: ARG002
        return "csharp"

    def extract_candidate_technologies(self, text):  # noqa: ARG002
        # Эмулируем промах keyword prefilter.
        return set()


class _NoopTextAnalyzer:
    def process_text(self, text):  # noqa: ARG002
        return []


class _TokenizingTextAnalyzer:
    def __init__(self):
        self.last_source = ""

    def process_text(self, text):
        self.last_source = str(text or "")
        return re.findall(r"[a-zA-Zа-яА-Я0-9#+]+", self.last_source.lower())


@pytest.mark.asyncio
async def test_find_technology_fallback_matches_csharp_with_symbol():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "Требуется опыт C# и ASP.NET"},
        technology="C#",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1
    assert any(match.get("field") == "description" for match in result["matches"])


@pytest.mark.asyncio
async def test_find_technology_fallback_matches_csharp_with_cyrillic_letter():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "Разработка приложений на языке С#"},
        technology="C#",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1


@pytest.mark.asyncio
async def test_find_technology_fallback_csharp_query_matches_csharp_symbol_notation():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "Разработка приложений на языке C#"},
        technology="csharp",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1


@pytest.mark.asyncio
async def test_find_technology_skips_keyword_prefilter_for_symbol_technology():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_KnownCSharpPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "Ищем разработчика C#"},
        technology="C#",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1


@pytest.mark.asyncio
async def test_find_technology_uses_fallback_when_known_pattern_misses_cyrillic_variant():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_KnownCSharpPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "Разработка приложений на языке С#"},
        technology="C#",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1


@pytest.mark.asyncio
async def test_find_technology_fallback_csharp_symbol_query_matches_word_notation():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "We need a C sharp developer"},
        technology="C#",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1


@pytest.mark.asyncio
async def test_find_technology_fallback_unknown_long_term_matches_inside_token():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "Тестовый стек: pytest, unittest, integration testing"},
        technology="test",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 3


@pytest.mark.asyncio
async def test_find_technology_fallback_unknown_short_term_keeps_word_boundaries():
    matcher = PatternMatcher(text_analyzer=_NoopTextAnalyzer(), patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "We use google cloud platform"},
        technology="go",
        search_fields=["description"],
    )

    assert result["found"] is False
    assert result["match_count"] == 0


@pytest.mark.asyncio
async def test_find_technology_css_ignores_matches_from_html_css_js_noise():
    matcher = PatternMatcher(text_analyzer=_NoopTextAnalyzer(), patterns_loader=_UnknownTechPatternsLoader())

    noisy_branded = """
    <style type="text/css">.swiper-container { margin: 0 auto; }</style>
    <script>function x(){ return $.css('padding-left'); }</script>
    <div data-file="/assets/main.css">Branded layout</div>
    """
    result = await matcher.find_technology(
        text={"branded_description": noisy_branded},
        technology="css",
        search_fields=["branded_description"],
    )

    assert result["found"] is False
    assert result["match_count"] == 0


@pytest.mark.asyncio
async def test_find_technology_css_still_matches_visible_human_text():
    matcher = PatternMatcher(text_analyzer=_NoopTextAnalyzer(), patterns_loader=_UnknownTechPatternsLoader())

    result = await matcher.find_technology(
        text={"description": "<p>Требования: HTML, CSS, JavaScript</p>"},
        technology="css",
        search_fields=["description"],
    )

    assert result["found"] is True
    assert result["match_count"] >= 1


@pytest.mark.asyncio
async def test_find_technology_css_normalized_fallback_uses_sanitized_text():
    text_analyzer = _TokenizingTextAnalyzer()
    matcher = PatternMatcher(text_analyzer=text_analyzer, patterns_loader=_UnknownTechPatternsLoader())

    noisy_branded = """
    <script>const x = '.css'; return $.css('padding');</script>
    <div data-file='/assets/main.css'>layout</div>
    """
    result = await matcher.find_technology(
        text={"branded_description": noisy_branded},
        technology="css",
        search_fields=["branded_description"],
    )

    assert result["found"] is False
    assert result["match_count"] == 0
    assert "css" not in text_analyzer.last_source.lower()
