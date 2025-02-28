from data_ingestion.src.f1_api import F1API
import pytest
import requests
import json
from pathlib import Path

# ---------- Fixtures ----------
@pytest.fixture(scope="module")
def f1_api():
    """Single instance of F1API with cleanup"""
    api = F1API()
    return api

@pytest.fixture(params=["2024"], ids=["season=2024"])
def season(request):
    return request.param

@pytest.fixture(params=[1], ids=["round=1"])
def race_round(request):
    return request.param

@pytest.fixture(params=[
    ("results", "season"),
    ("qualifying", "season"),
    ("sprint", "season"),
    ("laps", "race"),
    ("pitstops", "race")
], ids=lambda p: f"{p[0]}-{p[1]}")
def category_type(request):
    """Parameterize both category and its type"""
    return request.param

@pytest.fixture
def expected_data(category_type):
    category, data_type = category_type
    path = Path(f"tests/ingestion_test_data/{category}.json")
    if not path.exists():
        pytest.skip(f"Missing test data for {category} ({data_type})")
    with open(path) as f:
        return json.load(f)

@pytest.fixture
def api_url(category_type, f1_api, season, race_round):
    category, data_type = category_type
    
    if data_type == "season":
        return f1_api.base_url_season_result(
            category=category,
            season=season
        )
    elif data_type == "race":
        return f1_api.base_url_race_data(
            category=category,
            season=season,
            race_round=race_round
        )
    pytest.fail(f"Unknown data type: {data_type}")


# ---------- Tests ----------
def test_api_response_matches_stored_data(
    api_url, 
    expected_data,
):
    """
    Test that actual API responses match stored test data
    using pytest-recording for network call management
    """
    response = requests.get(api_url)
    response.raise_for_status()
    
    result = response.json()
    
    # Use custom comparison if needed
    print(type(result))
    print(type(expected_data))
    assert result["MRData"]["RaceTable"]["Races"][0].keys() == expected_data["MRData"]["RaceTable"]["Races"][0].keys()

def test_url_generation(api_url, category_type):
    """Validate URL structure without network calls"""
    category, data_type = category_type
    if data_type == "season":
        assert f"{category}" in api_url
    elif data_type == "race":
        assert f"{category}" in api_url

#add commentary to test my CI workflow