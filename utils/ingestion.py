import logging
from datetime import date, timedelta
import json
from data_ingestion.src.f1_api import F1API
from utils.s3_client import S3Client
from utils.s3_utils import read_object_into_json

def get_last_date(f1_schedule: list) -> str:
    """
    Retrieves the latest race date from the F1 schedule.

    Args:
        f1_schedule (list): A list of race schedules, where each item is a dictionary containing a "date" key.

    Returns:
        str: The latest date in the schedule in string format (YYYY-MM-DD).
    """

    date = max([schedule["date"] for schedule in f1_schedule])
    return date

def fetch_f1_schedule(s3_client: S3Client, config: dict, f1_api: F1API) -> None:
    """
    Retrieves the F1 schedule from an S3 bucket or updates it if the stored schedule is outdated.

    Args:
        s3_client (S3Client): An instance of the S3 client to interact with AWS S3.
        config (dict): Configuration dictionary containing required settings (e.g., S3 bucket name).
        f1_api (F1API): An instance of the F1API class to fetch the latest schedule if needed.

    Behavior:
        - Reads the F1 schedule from an S3 JSON file.
        - Checks if the last race date is outdated.
        - If outdated, updates the schedule via the F1 API and writes the new data to S3.
        - Otherwise, uses the existing schedule.
    """

    s3_f1_schedule_file = f"{f1_api.folder_name}/f1_schedule.json"

    f1_schedule = read_object_into_json(
        s3_client=s3_client, object_key=s3_f1_schedule_file
    )

    last_schedule_date = get_last_date(f1_schedule)
    today = date.today().strftime("%Y-%m-%d")

    if last_schedule_date <= today:
        logging.info("Updating F1 schedule...")
        f1_api.fetch_f1_seasons_schedule()
        s3_client.write_object(
            bucket_name=config["s3_bucket"],
            object_key=s3_f1_schedule_file,
            data=json.dumps(f1_api.f1_schedule),
        )
    else:
        logging.info("Using existing F1 schedule.")
        f1_api.f1_schedule = f1_schedule


def process_race_data(f1_api: F1API, LOOKBACK_DAYS: int, get_all: bool) -> None:
    """
    Processes and fetches race data within a specified date range.

    Args:
        f1_api (F1API): An instance of the F1API class to fetch race results.
        LOOKBACK_DAYS (int): The number of days to look back for race data.
        get_all (bool): If True, processes all races; otherwise, filters races within the specified date range.

    Behavior:
        - If get_all is False, filters races from the last LOOKBACK_DAYS.
        - If get_all is True, processes all races in the schedule.
        - Fetches the race results and handles any potential errors.
    """

    if get_all == False:
        today = date.today()
        date_range_start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        date_range_end = today.strftime("%Y-%m-%d")

        f1_api.pending_races = [
            race
            for race in f1_api.f1_schedule
            if date_range_start <= race["date"] <= date_range_end
        ]
    else:
        f1_api.pending_races = f1_api.f1_schedule

    logging.info("Fetching race results...")
    try: 
        f1_api.fetch_data()
    except Exception as e:
        logging.error(f"Error fetching season results: {e}")
