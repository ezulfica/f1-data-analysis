import requests
import yaml
from time import sleep
from math import ceil
import logging
from tqdm import tqdm
import json
from pathlib import Path

class F1API:

    def __init__(self, config_file="config/settings.yaml"):
        """
        Initializes the F1API instance by loading configuration settings.

        :param config_file: Path to the configuration file (default is 'config/settings.yaml')
        """
        with open(config_file) as file:
            self.settings = yaml.safe_load(file)

        self.base_url = self.settings["f1_api"]["base_url"]
        self.season_category = self.settings["f1_api"]["season_category"]
        self.round_category = self.settings["f1_api"]["round_category"]
        self.f1_schedule = None
        self.pending_races = None
        self.f1_seasons_results = None
        self.f1_races_data = None
        self.folder_name = "raw"
    
    @staticmethod
    def get_pagination_offsets(total:str) -> list:
        """
        Calculates the pagination offsets based on the total number of items.
        This is useful for fetching large datasets from paginated API responses.

        :param total: Total number of items (as a string) to fetch
        :return: List of offsets (each offset is a multiple of 100)
        """

        total_races = int(total)
        if total_races > 0:
            query_numbers = max(1, ceil(total_races / 100))
            offsets = [x * 100 for x in range(1, query_numbers)]
        else:
            offsets = []
        return offsets
    
    def build_param_url(self, url: str, total: str) -> list:
        """
        Generates paginated URLs for fetching data from the API.

        :param url: Base URL for the API request
        :param total: Total number of items to retrieve (used for pagination)
        :return: List of paginated URLs
        """
        try : 
            offsets = self.get_pagination_offsets(total)
            url_list = [f"{url}?limit=100&offset={offset}" for offset in offsets]
            return url_list
        except requests.RequestException as e:
            logging.error(f"Failed to fetch data from {url}: {e}")
            return []

    def fetch_circuit_schedule(self, offset: int) -> list:
        """
        Fetches the F1 circuit schedule (year and round) for a specific offset.
        The offset helps paginate through multiple requests.

        :param offset: The pagination offset
        :return: List of races at a given offset (or an empty list in case of an error)
        """
        url = f"{self.base_url}?offset={offset}&limit=100"
        try:
            sleep(0.3)
            with requests.get(url, timeout=10) as response:
                response.raise_for_status()  # Raises an HTTP error for bad responses (4xx or 5xx)
                data = response.json()
                return data["MRData"]["RaceTable"]["Races"]
        except KeyError:
            print(f"Unexpected response format for offset {offset}")
        except Exception as e:
            print(f"An unexpected error occurred for offset {offset}: {e}")
        return []  # Return an empty dict if there's an error

    def fetch_f1_seasons_schedule(self) -> list:
        """
        Fetches the F1 circuit schedule for the entire season, handling pagination.

        This method fetches the initial response and then makes additional requests 
        for paginated data.

        :return: A list of races from the F1 season
        """
        first_response = requests.get(self.base_url, timeout=10).json()
        total = first_response['MRData']['total']
        offsets = self.get_pagination_offsets(total)
        f1_list = [self.fetch_circuit_schedule(offset) for offset in offsets]
        f1_list.append(first_response["MRData"]["RaceTable"]["Races"])
        self.f1_schedule = [race for races in f1_list for race in races]

    def base_url_data(self, category, season, race_round=None) -> str:
        """
        Builds a full URL based on category, season, and (optionally) race round.

        :param category: The category of the data (e.g., 'results', 'qualifying')
        :param season: The F1 season
        :param race_round: The specific race round (optional)
        :return: The complete URL to access the data
        """
        return f"{self.base_url}{season}/{race_round + '/' if race_round else ''}{category}"
    
    def build_base_url_data(self) -> list:
        """
        Builds the URLs for both race-based and season-based data.

        This method organizes races into groups by season and builds URLs 
        for each race or season's data.

        :return: List of dictionaries containing category, season, and URLs
        """
        data_source = [(fone["season"], fone["round"]) for fone in self.pending_races]
        seasons = list(set(fone["season"] for fone in self.pending_races))
        
        grouped_race_data = {}
        for season, race_round in data_source:
            grouped_race_data.setdefault(season, []).append(race_round)
        
        results_url = []
        
        # Build race-based URLs
        if self.round_category:
            for season, race_rounds in grouped_race_data.items():
                for category in self.round_category:
                    results_url.append({
                        "category": category,
                        "season": season,
                        "rounds": race_rounds,
                        "urls": [self.base_url_data(category, season, r) for r in race_rounds],
                    })
        # Build season-based URLs
        if self.season_category: 
            for season in seasons:
                for category in self.season_category:
                    results_url.append({
                        "category": category,
                        "season": season,
                        "urls": [self.base_url_data(category, season)],
                    })
        
        return results_url
    
    def fetch_data(self) -> None:
        """
        Fetches race data for all required categories and seasons.
        
        This method iterates over all parameters and fetches the necessary data 
        using the `build_race_data` method for each set of parameters.
        """
        params = self.build_base_url_data()
        data = [self.build_race_data(param)
                for param in tqdm(params, desc="fetching races data")
        ]
    
    def build_race_data(self, params: dict) -> None:
        """
        Builds and saves race data based on the provided parameters.

        :param params: A dictionary containing the category, season, and URLs to fetch data
        """
        urls = params["urls"]
        season = params["season"]
        category = params["category"]
        filename = f"{self.folder_name}/{category}/{season}_{category}.json"

        first_responses = self.fetch_initial_responses(urls)
        if not first_responses:
            return

        merged_data = first_responses.copy()
        url_params = self.get_additional_urls(first_responses)
        
        if url_params:
            merged_data.extend(self.fetch_additional_data(url_params))

        self.save_data_to_file(filename, merged_data)

    def fetch_initial_responses(self, urls: list) -> list:
        """
        Fetches initial responses from the given list of URLs.

        :param urls: A list of URLs to fetch data from
        :return: List of successful responses
        """
        responses = []
        for url in urls:
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                response_json = response.json()
                if response_json['MRData']['total'] != '0':
                    responses.append(response_json)
            except requests.RequestException as e:
                logging.error(f"Failed to fetch data from {url}: {e}")
        return responses

    def get_additional_urls(self, first_responses:list) -> list:
        """
        Generates additional paginated URLs based on the initial responses.

        :param first_responses: A list of the first responses from the API
        :return: A list of URLs for additional pages of data
        """
        return [
            offset
            for response in first_responses
            for offset in self.build_param_url(
                response['MRData']['url'], response['MRData']['total']
                )
        ]

    def fetch_additional_data(self, url_params: list) -> list:
        """
        Fetches additional data from paginated URLs.

        :param url_params: A list of paginated URLs to fetch additional data from
        :return: A list of additional data fetched from the URLs
        """
        additional_data = []
        for url in url_params:
            sleep(0.3)
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                additional_data.append(response.json())
            except requests.RequestException as e:
                logging.error(f"Failed to fetch data from {url}: {e}")
        return additional_data

    def save_data_to_file(self, filename, data):
        folder = "/".join(filename.split("/")[:-1])
        Path(folder).mkdir(parents=True, exist_ok=True)
        with open(filename, "w") as file:
            json.dump(data, file)
    
   
# -- ASYNC TEST --
# MAX_CONCURRENT_REQUESTS = 1
# semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# async def fetch_f1_season_info(session, offset):

#     """Query the API to get all the race year and round
#     using the offset parameters with async method"""

#     url = f'{base_url}/?offset={offset}&limit=100'
#     try:
#         await asyncio.sleep(10)  # Simulating delay without blocking
#         async with session.get(url, timeout=10) as response:
#             response.raise_for_status()  # Raises an HTTP error for bad responses (4xx or 5xx)
#             data = await response.json()
#             return data["MRData"]["RaceTable"]["Races"]

#     except ClientError as e:
#         print(f"Network error occurred for offset {offset}: {e}")
#     except asyncio.TimeoutError:
#         print(f"Request timed out for offset {offset}")
#     except KeyError:
#         print(f"Unexpected response format for offset {offset}")
#     except Exception as e:
#         print(f"An unexpected error occurred for offset {offset}: {e}")
#     return []  # Return an empty list if there's an error

# async def f1_seasons(offsets : list) :
#     async with aiohttp.ClientSession() as session:
#         tasks = [fetch_f1_season_info(session, offset) for offset in offsets]
#         results = await asyncio.gather(*tasks)
#         return results
