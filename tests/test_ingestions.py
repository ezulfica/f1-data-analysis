from data_ingestion.src.f1_api import F1API
import pytest
import requests
import json
from pathlib import Path
from typing import Tuple, Dict

# ---------- Fixtures ----------

@pytest.fixture(scope="module")
def f1_api() -> F1API:
    """Single instance of F1API that can be used across multiple tests in the module"""
    api = F1API()  # Initialize F1API class (assumed to be defined elsewhere)
    return api  # This instance will be passed to any test requiring it


@pytest.fixture(params=["2024"], ids=["season=2024"])
def season(request) -> str:
    """Fixture to provide the season parameter (2024 in this case) for parameterized tests"""
    return request.param  # Returns the value of the current parameter ("2024")


@pytest.fixture(params=["1"], ids=["round=1"])
def race_round(request) -> str:
    """Fixture to provide the race round parameter (round 1 in this case) for parameterized tests"""
    return request.param  # Returns the value of the current parameter ("1")


@pytest.fixture(params=[
    ("results", "season"),
    ("qualifying", "season"),
    ("sprint", "season"),
    ("driverstandings", "season"),
    ("constructorstandings", "season"),
    ("laps", "race"),
    ("pitstops", "race")
], ids=lambda p: f"{p[0]}-{p[1]}")
def category_type(request) -> Tuple[str, str]:
    """
    Fixture to parameterize the category and type of data we are testing (e.g. results, qualifying)
    This helps test various categories (e.g., "season", "race") in different contexts with the same test logic.
    """
    return request.param  # Returns a tuple (category, type)


@pytest.fixture
def expected_data(category_type: Tuple[str, str]) -> Dict:
    """
    Fixture to load expected test data from files for comparison in tests
    The data is loaded from a directory (`tests/ingestion_test_data/`) based on the category and type of data.
    """
    category, data_type = category_type
    path = Path(f"tests/ingestion_test_data/{category}.json")
    
    # Skip the test if the corresponding file does not exist
    if not path.exists():
        pytest.skip(f"Missing test data for {category} ({data_type})")
    
    # Load the test data from the file and return it
    with open(path) as f:
        return json.load(f)


@pytest.fixture
def api_url(
    category_type: Tuple[str, str], 
    f1_api: F1API, 
    season: str, 
    race_round: str
) -> str:
    """
    Fixture to generate the correct API URL based on the category, data type, season, and race round
    This helps in testing the URL generation logic separately from actual network requests.
    """
    category, data_type = category_type
    
    # For "season" type, generate a URL for season-related data
    if data_type == "season":
        return f1_api.base_url_data(
            category=category,
            season=season
        )
    
    # For "race" type, generate a URL for race-related data
    elif data_type == "race":
        return f1_api.base_url_data(
            category=category,
            season=season,
            race_round=race_round
        )
    
    else:
        # If the data type is unknown, fail the test
        return pytest.fail(f"Unknown data type: {data_type}")


# ---------- Tests ----------

def test_api_response_matches_stored_data(
    api_url: str, 
    expected_data: Dict
):
    """
    Test that actual API responses match stored test data using pytest-recording for network call management.
    This is a typical integration test that ensures the API response structure matches the expected output.
    """
    # Send a GET request to the generated API URL
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an error if the response status is not successful
    
    # Convert the response content into a JSON object
    result = response.json()
    
    # Compare the keys in the response data with the expected data
    # You can customize this part to check for deeper comparisons if needed
    print(type(result))  # Debugging print statement (can be removed in production)
    print(type(expected_data))  # Debugging print statement (can be removed in production)
    
    # Ensure that the keys of the first race in the result match the keys in the expected data
    assert result["MRData"]["RaceTable"]["Races"][0].keys() == expected_data["MRData"]["RaceTable"]["Races"][0].keys()


def test_url_generation(api_url: str, category_type: Tuple[str, str]):
    """
    Validate the URL generation logic without making network calls.
    This ensures that the URL structure is correct before testing against the actual API.
    """
    category, data_type = category_type
    
    # Check that the category is present in the URL (basic URL validation)
    if data_type == "season":
        assert f"{category}" in api_url  # Validate that the category is included in the URL
    
    elif data_type == "race":
        assert f"{category}" in api_url  # Validate that the category is included in the URL
