import requests
import yaml
from time import sleep 
from math import ceil

class F1API() : 

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
        self.folder_name = None

    # ---- F1 SCHEDULE ---- 
    def get_pagination_offsets(self) -> list :
        """In order to get all the value from the API, 
        the offset parameters is used. 
        Offset value is a multiple of 100, 
        which is the number of races that we can fetch"""

        response = requests.get(self.base_url)
        if response.status_code == 200:
            total_races = int(response.json()["MRData"]["total"])
            query_numbers = ceil(total_races / 100)
            self.offsets = [x*100 for x in range(0, query_numbers)]
        else : 
            self.offsets = None

    def fetch_circuit_schedule(self ,offset : int) -> list :

        """Retrieve F1 circuit schedule (year and round) for a specific offset."""

        url = f'{self.base_url}/?offset={offset}&limit=100'
        try:
            sleep(0.5)
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

        self.get_pagination_offsets()  
        f1_list = [self.fetch_circuit_schedule(offset) for offset in self.offsets]
        unflatted_races = [race for races in f1_list for race in races] 
        self.f1_schedule = unflatted_races

    # ---- F1 RACE DATA ---- 
    def base_url_race_data(self ,category, season, race_round) :
        """endpoint url for pits and laps data"""
        return f'{self.base_url}{season}/{race_round}/{category}'  
    
    def build_url_race_data(self) : 
        """
        results = ["laps", "pitstop"]
        """
        seasons_and_rounds = [(fone['season'], fone['round']) for fone in self.pending_races]
        
        combinations = [(season, rounds, category) 
                        for (season, rounds) in seasons_and_rounds for category in self.round_category
                        ]
        
        result_url = [(category, season, race_round, self.base_url_race_data(category, season, race_round)) 
                      for season, race_round, category in combinations
                      ]
        
        return result_url  

    def build_race_data(self, params) :
        url =  params[-1]
        season = params[1]
        race_round = params[2]
        category = params[0]
        filename = f'{self.folder_name}/{category}/{season}_{race_round}_{category}.json' 
    
        with requests.get(url) as response : 
            if response.status_code == 200 : 
                data = response.json()
                return (filename, data)

    def fetch_races_data(self) : 
        params = self.build_url_race_data()
        data = [self.build_race_data(param) for param in params]
        self.f1_races_data = data

    # ---- F1 SEASON RESULTS ---- 

    def base_url_season_result(self, category, season) :
        """endpoint url for results, sprint, qualifying data"""
        return f'{self.base_url}{season}/{category}'
    
    def build_url_season_result(self) : 
     """
     results = ["results", "qualifying", "sprint"]
     """
     seasons = set([(fone['season']) for fone in self.pending_races])
     combinations = [(season, category) for season in set(seasons) for category in self.season_category]
     results_url = [(category, season, self.base_url_season_result(category, season)) for season, category in combinations]
     return results_url

    def build_races_results(self, params) :
     url =  params[-1]
     season = params[-2]
     category = params[0]

     filename = f'{self.folder_name}/{category}/{season}_{category}.json' 

     with requests.get(url) as response : 
          if response.status_code == 200 : 
               data = response.json()
               return (filename, data)
    
    def fetch_seasons_results(self) : 
        params = self.build_url_season_result()
        data = [self.build_races_results(param) for param in params]
        self.f1_seasons_results = data



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