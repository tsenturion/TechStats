import re
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

from app.routers.patterns import get_patterns_loader, router as patterns_router


class FakePatternsLoader:
    def __init__(self):
        self.patterns = {
            "python": {
                "name": "Python",
                "category": "programming_language",
                "patterns": [r"\bpython\b"],
                "aliases": ["py"],
                "description": "python tech",
                "weight": 1.0,
            },
            "react": {
                "name": "React",
                "category": "framework",
                "patterns": [r"\breact\b"],
                "aliases": ["reactjs"],
                "description": "react tech",
                "weight": 1.0,
            },
        }

    def get_all_patterns(self):
        return dict(self.patterns)

    def get_categories(self):
        return sorted({item["category"] for item in self.patterns.values()})

    def get_technologies_by_category(self, category):
        return [{**value, "id": key} for key, value in self.patterns.items() if value["category"] == category]

    def get_pattern(self, technology):
        tech = technology.lower()
        if tech in self.patterns:
            return self.patterns[tech]
        for data in self.patterns.values():
            if tech in [alias.lower() for alias in data.get("aliases", [])]:
                return data
        return None

    def get_compiled_pattern(self, technology):
        pattern = self.get_pattern(technology)
        if not pattern:
            return None
        return re.compile("|".join(pattern["patterns"]), re.IGNORECASE)

    def add_pattern(self, tech_id, name, patterns, category="other", aliases=None, weight=1.0, description=""):
        key = tech_id.lower()
        if key in self.patterns:
            return False
        self.patterns[key] = {
            "name": name,
            "category": category,
            "patterns": patterns,
            "aliases": aliases or [],
            "weight": weight,
            "description": description,
        }
        return True

    def remove_pattern(self, tech_id):
        return self.patterns.pop(tech_id.lower(), None) is not None

    async def save_and_cache(self):
        return True


def _client():
    app = FastAPI()
    app.include_router(patterns_router, prefix="/api/v1")
    loader = FakePatternsLoader()
    app.dependency_overrides[get_patterns_loader] = lambda: loader
    return TestClient(app), loader


def test_get_patterns_and_filter_by_category():
    client, _ = _client()

    all_response = client.get("/api/v1/patterns")
    assert all_response.status_code == 200
    assert all_response.json()["total_patterns"] == 2

    filtered = client.get("/api/v1/patterns", params={"category": "framework"})
    assert filtered.status_code == 200
    assert filtered.json()["total_patterns"] == 1
    assert filtered.json()["patterns"][0]["id"] == "react"


def test_get_single_pattern_and_404():
    client, _ = _client()
    ok = client.get("/api/v1/patterns/python")
    assert ok.status_code == 200
    assert ok.json()["compiled"] is True

    missing = client.get("/api/v1/patterns/missing")
    assert missing.status_code == 404


def test_add_update_delete_pattern_flow():
    client, loader = _client()

    missing_field = client.post("/api/v1/patterns", json={"id": "go"})
    assert missing_field.status_code == 400

    add_response = client.post(
        "/api/v1/patterns",
        json={"id": "go", "name": "Go", "patterns": [r"\bgo\b"], "aliases": ["golang"]},
    )
    assert add_response.status_code == 200
    assert "go" in loader.patterns

    update_response = client.put(
        "/api/v1/patterns/go",
        json={"name": "GoLang", "patterns": [r"\bgo\b", r"\bgolang\b"], "category": "programming_language"},
    )
    assert update_response.status_code == 200
    assert loader.patterns["go"]["name"] == "GoLang"

    delete_response = client.delete("/api/v1/patterns/go")
    assert delete_response.status_code == 200
    assert "go" not in loader.patterns


def test_categories_search_and_stats_endpoints():
    client, _ = _client()

    categories = client.get("/api/v1/patterns/categories")
    assert categories.status_code == 200
    assert categories.json()["total_categories"] >= 1

    search = client.post("/api/v1/patterns/search", json={"query": "py"})
    assert search.status_code == 200
    assert search.json()["total_found"] >= 1

    search_bad = client.post("/api/v1/patterns/search", json={})
    assert search_bad.status_code == 400

    stats = client.get("/api/v1/patterns/stats")
    assert stats.status_code == 200
    assert stats.json()["total_patterns"] == 2
