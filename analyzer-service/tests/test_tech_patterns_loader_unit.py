import pytest

from app.tech_patterns import TechPatternsLoader


@pytest.mark.asyncio
async def test_default_patterns_include_alias_lookup_and_compiled_regex():
    loader = TechPatternsLoader()
    await loader._create_default_patterns()

    python_pattern = loader.get_pattern("python")
    alias_pattern = loader.get_pattern("py")
    compiled = loader.get_compiled_pattern("python")

    assert python_pattern is not None
    assert alias_pattern is not None
    assert python_pattern["name"] == alias_pattern["name"]
    assert compiled is not None
    assert compiled.search("Need Python 3 developer")

    csharp_pattern = loader.get_pattern("c#")
    csharp_compiled = loader.get_compiled_pattern("c#")
    assert csharp_pattern is not None
    assert csharp_pattern["name"] == "C#"
    assert csharp_compiled is not None
    assert csharp_compiled.search("Требуется опыт C# и ASP.NET")

    angular_pattern = loader.get_pattern("angular")
    vue_pattern = loader.get_pattern("vue")
    assert angular_pattern is not None
    assert angular_pattern["name"] == "Angular"
    assert vue_pattern is not None
    assert vue_pattern["name"] == "Vue"

    js_compiled = loader.get_compiled_pattern("javascript")
    assert js_compiled is not None
    assert js_compiled.search("nodejs backend")
    assert not js_compiled.search("vue developer")


@pytest.mark.asyncio
async def test_get_pattern_requires_exact_id_or_alias_without_prefix_matching():
    loader = TechPatternsLoader()
    await loader._create_default_patterns()

    python_by_prefix = loader.get_pattern("pyth")
    no_false_positive_for_go = loader.get_pattern("go")

    assert python_by_prefix is None
    assert no_false_positive_for_go is None


def test_add_and_remove_pattern_flow():
    loader = TechPatternsLoader()
    loader.patterns = {}
    loader.categories = set()
    loader.aliases = {}
    loader.compiled_patterns = {}

    added = loader.add_pattern(
        tech_id="go",
        name="Go",
        patterns=[r"\bgo\b", r"\bgolang\b"],
        category="programming_language",
        aliases=["golang"],
    )
    assert added is True
    assert loader.get_pattern("golang")["name"] == "Go"
    assert loader.get_compiled_pattern("go").search("golang backend")

    removed = loader.remove_pattern("go")
    assert removed is True
    assert loader.get_pattern("go") is None


def test_add_pattern_rejects_invalid_regex_and_duplicate():
    loader = TechPatternsLoader()
    loader.patterns = {}
    loader.categories = set()
    loader.aliases = {}
    loader.compiled_patterns = {}

    assert loader.add_pattern("java", "Java", [r"\bjava\b"]) is True
    assert loader.add_pattern("java", "Java", [r"\bjava\b"]) is False

    assert loader.add_pattern("bad", "Bad", ["("]) is False


def test_normalize_patterns_schema_splits_javascript_and_framework_aliases():
    loader = TechPatternsLoader()
    loader.patterns = {
        "javascript": {
            "name": "JavaScript",
            "category": "programming_language",
            "patterns": [r"\bjavascript\b", r"\bangular\b", r"\bvue\b", r"\breact\b"],
            "aliases": ["js", "angular", "vue", "reactjs"],
            "weight": 1.0,
            "description": "",
        }
    }
    loader.categories = {"programming_language"}

    changed = loader._normalize_patterns_schema()
    loader._build_aliases()

    assert changed is True
    js_payload = loader.patterns["javascript"]
    assert r"\bangular\b" not in js_payload["patterns"]
    assert r"\bvue\b" not in js_payload["patterns"]
    assert r"\breact\b" not in js_payload["patterns"]
    assert "angular" not in js_payload["aliases"]
    assert "vue" not in js_payload["aliases"]
    assert "reactjs" not in js_payload["aliases"]
    assert loader.aliases["angularjs"] == "angular"
    assert loader.aliases["vuejs"] == "vue"
    assert loader.aliases["reactjs"] == "react"
