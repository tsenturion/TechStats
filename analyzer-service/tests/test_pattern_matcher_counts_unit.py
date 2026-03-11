import pytest

from app.analyzer import PatternMatcher


class SpyMatcher(PatternMatcher):
    def __init__(self, payload):
        super().__init__(text_analyzer=None, patterns_loader=None)
        self.payload = payload
        self.calls = []

    async def find_technology(self, text, technology, search_fields=None):
        self.calls.append(
            {
                "text": text,
                "technology": technology,
                "search_fields": list(search_fields or []),
            }
        )
        return dict(self.payload)


def test_extract_key_skills_text_handles_hh_variants():
    matcher = PatternMatcher(text_analyzer=None, patterns_loader=None)

    from_list = matcher._extract_key_skills_text(
        {"key_skills": [{"name": "Python"}, {"name": "FastAPI"}]}
    )
    from_dict = matcher._extract_key_skills_text({"key_skills": {"name": "Docker"}})
    from_string = matcher._extract_key_skills_text({"key_skills": "SQL"})
    from_empty = matcher._extract_key_skills_text({})

    assert from_list == "Python FastAPI"
    assert from_dict == "Docker"
    assert from_string == "SQL"
    assert from_empty == ""


@pytest.mark.asyncio
async def test_analyze_vacancy_splits_text_and_key_skills_matches():
    matcher = SpyMatcher(
        {
            "found": True,
            "technology": "Python",
            "pattern_name": "Python",
            "category": "programming_language",
            "matches": [
                {"field": "description"},
                {"field": "key_skills"},
                {"field": "key_skills"},
            ],
            "match_count": 3,
            "search_fields": ["title", "snippet", "description", "key_skills"],
        }
    )

    vacancy = {
        "id": "123",
        "name": "Python Developer",
        "alternate_url": "https://hh.ru/vacancy/123",
        "description": "Need Python developer",
        "snippet": {
            "requirement": "FastAPI",
            "responsibility": "Build services",
        },
        "key_skills": [{"name": "Python"}, {"name": "FastAPI"}],
    }

    result = await matcher.analyze_vacancy(vacancy, "Python", exact_search=False)

    assert result["has_technology"] is True
    assert result["match_count"] == 3
    assert result["text_match_count"] == 1
    assert result["key_skills_match_count"] == 2

    assert matcher.calls
    first_call = matcher.calls[0]
    assert "key_skills" in first_call["search_fields"]
    assert first_call["text"]["key_skills"] == "Python FastAPI"


@pytest.mark.asyncio
async def test_analyze_vacancy_includes_branded_description_fields():
    matcher = SpyMatcher(
        {
            "found": False,
            "technology": "C#",
            "pattern_name": "C#",
            "category": "programming_language",
            "matches": [],
            "match_count": 0,
            "search_fields": ["title", "snippet", "description", "branded_description", "vacancy_constructor_template", "key_skills"],
        }
    )

    vacancy = {
        "id": "777",
        "name": "Инженер-программист С#",
        "description": "",
        "branded_description": "<div>Мы ищем разработчика на языке С#</div>",
        "vacancy_constructor_template": {"template": "С# inside constructor"},
    }

    await matcher.analyze_vacancy(vacancy, "C#", exact_search=False)

    assert matcher.calls
    first_call = matcher.calls[0]
    assert "branded_description" in first_call["search_fields"]
    assert "vacancy_constructor_template" in first_call["search_fields"]
    assert "С#" in first_call["text"]["branded_description"]
    assert "С#" in first_call["text"]["vacancy_constructor_template"]
