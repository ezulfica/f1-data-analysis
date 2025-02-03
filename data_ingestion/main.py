import logging
from dotenv import dotenv_values
from data_ingestion.src.f1_api import F1API  # Absolute import
from utils.s3_utils import connect_s3, upload_results
from utils.ingestion import fetch_f1_schedule, process_race_data

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
CONFIG_PATH = "config/.env"
LOOKBACK_DAYS = 5

#main function
def main():
    """Main execution function."""
    config = dotenv_values(CONFIG_PATH)
    s3_client = connect_s3(config)
    f1_api = F1API()
    fetch_f1_schedule(s3_client, config, f1_api)
    process_race_data(f1_api, config, LOOKBACK_DAYS, get_all=False)
    upload_results(s3_client, f1_api.folder_name)

if __name__ == "__main__":
    main()