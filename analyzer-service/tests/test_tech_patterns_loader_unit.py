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
