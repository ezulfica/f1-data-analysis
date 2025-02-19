import requests
import yaml
from time import sleep
from math import ceil
import logging
from tqdm import tqdm
import json


class F1API:

    def __init__(self, config_file="config/settings.yaml"):
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
    def get_pagination_offsets(url) -> list:
        """In order to get all the value from the API,
        the offset parameters is used.
        Offset value is a multiple of 100,
        which is the number of races that we can fetch"""

        response = requests.get(url)
        if response.status_code == 200:
            total_races = int(response.json()["MRData"]["total"])
            if total_races > 0:
                query_numbers = max(1, ceil(total_races / 100))
                offsets = [x * 100 for x in range(query_numbers)]
            else:
                offsets = []
        else:
            offsets = []
        return offsets

    def build_param_url(self, url_param: dict) -> None:
        url = url_param["url"]
        if (not url) is False:
            sleep(0.3)
            offsets = self.get_pagination_offsets(url)
            if (not offsets) is False:
                url_param["url"] = [
                    f"{url}/?limit=100&offset={offset}" for offset in offsets
                ]
                return url_param

    # -----------------------------------------------------------------------------
    # ------------------- F1 SCHEDULE ---- --------------------------------- ------
    # -----------------------------------------------------------------------------
    def fetch_circuit_schedule(self, offset: int) -> list:
        """Retrieve F1 circuit schedule (year and round) for a specific offset."""

        url = f"{self.base_url}/?offset={offset}&limit=100"
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
        """Retrieve F1 circuit schedule (year and round) given a list of offsets."""

        offsets = self.get_pagination_offsets(self.base_url)
        f1_list = [self.fetch_circuit_schedule(offset) for offset in offsets]
        unflatted_races = [race for races in f1_list for race in races]
        self.f1_schedule = unflatted_races

    # -----------------------------------------------------------------------------
    # ------ ------- ---- ---- - ---- -- F1 RACE DATA ------- ---- ---- ---- ---- -
    # -----------------------------------------------------------------------------
    def base_url_race_data(self, category, season, race_round):
        """endpoint url for pits and laps data"""
        return f"{self.base_url}{season}/{race_round}/{category}"

    def build_base_url_race_data(self):
        """
        results = ["laps", "pitstop"]
        """
        seasons_and_rounds = [
            (fone["season"], fone["round"]) for fone in self.pending_races
        ]

        combinations = [
            (season, rounds, category)
            for (season, rounds) in seasons_and_rounds
            for category in self.round_category
        ]

        results_url = [
            {
                "category": category,
                "season": season,
                "round": race_round,
                "url": self.base_url_race_data(category, season, race_round),
            }
            for season, race_round, category in combinations
        ]

        return results_url

    def build_race_data(self, params):

        url_params = params["url"]
        season = params["season"]
        category = params["category"]
        filename = f"{self.folder_name}/{category}/{season}_{category}.json"

        merged_data = []
        for url in url_params:
            sleep(0.3)
            try:
                response = requests.get(
                    url, timeout=10
                )  # Added timeout for better handling
                response.raise_for_status()  # Raise an error for bad responses (4xx/5xx)
                merged_data.append(response.json())
            except requests.RequestException as e:
                logging.error(f"Failed to fetch data from {url}: {e}")

        with open(filename, "w") as file:
            return json.dump(merged_data, file)

    def build_url_race_data(self):
        base_url = self.build_base_url_race_data()
        results_url = [
            self.build_param_url(url) for url in tqdm(base_url, desc="Processing URLs")
        ]

        clean_results = [urls for urls in results_url if (not urls) is False]

        season_and_category = set(
            [(race["season"], race["category"]) for race in clean_results]
        )
        url_by_seasons_and_category = [
            {
                "season": s_c[0],
                "category": s_c[1],
                "url": [
                    race["url"]
                    for race in clean_results
                    if race["season"] == s_c[0] and race["category"] == s_c[1]
                ],
            }
            for s_c in season_and_category
        ]

        final_list = [
            {
                "season": s_c["season"],
                "category": s_c["category"],
                "url": [url for urls in s_c["url"] for url in urls],
            }
            for s_c in url_by_seasons_and_category
        ]

        self.race_data_list = final_list

    def fetch_races_data(self):
        self.build_url_race_data()
        params = self.race_data_list
        data = [
            self.build_race_data(param)
            for param in tqdm(params, desc="fetching races data")
        ]

    # ------------------------------------------------------------------
    # ------- ---- ---- ---- - F1 SEASON RESULTS -------- ---- ---- ----
    # ------------------------------------------------------------------

    def base_url_season_result(self, category, season):
        """endpoint url for results, sprint, qualifying data"""
        return f"{self.base_url}{season}/{category}"

    def build_base_url_season_result(self):
        """
        results = ["results", "qualifying", "sprint"]
        """
        seasons = set([(fone["season"]) for fone in self.pending_races])
        combinations = [
            (season, category)
            for season in set(seasons)
            for category in self.season_category
        ]

        results_url = [
            {
                "category": category,
                "season": season,
                "url": self.base_url_season_result(category, season),
            }
            for season, category in combinations
        ]

        return results_url

    def build_races_results(self, params):
        url_params = params["url"]
        season = params["season"]
        category = params["category"]
        filename = f"{self.folder_name}/{category}/{season}_{category}.json"

        merged_data = []
        for url in url_params:
            sleep(0.05)
            try:
                response = requests.get(
                    url, timeout=10
                )  # Added timeout for better handling
                response.raise_for_status()  # Raise an error for bad responses (4xx/5xx)
                merged_data.append(response.json())

            except requests.RequestException as e:
                logging.error(f"Failed to fetch data from {url}: {e}")

        with open(filename, "w") as file:
            return json.dump(merged_data, file)

    def build_url_season_result(self):
        base_url = self.build_base_url_season_result()
        results_url = [
            self.build_param_url(url) for url in tqdm(base_url, desc="Processing URLs")
        ]
        clean_base = [urls for urls in results_url if (not urls) is False]
        return clean_base

    def fetch_seasons_results(self):
        params = self.build_url_season_result()
        data = [
            self.build_races_results(param)
            for param in tqdm(params, desc="fetching results")
        ]


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
