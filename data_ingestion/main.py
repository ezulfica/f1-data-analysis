import logging
from data_ingestion.src.f1_api import F1API
from utils.config_utils import load_config
from utils.s3_utils import connect_s3, upload_results
from data_ingestion.utils.func_utils import fetch_f1_schedule, process_race_data

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
CONFIG_PATH = "config/.env"
LOOKBACK_DAYS = 50

#main function
def main():
    """Main execution function."""
    config = load_config(CONFIG_PATH)
    s3_client = connect_s3(config)
    f1_api = F1API()
    fetch_f1_schedule(s3_client, config)
    process_race_data(f1_api, config, LOOKBACK_DAYS)
    upload_results(s3_client, f1_api, config["s3_bucket"])

if __name__ == "__main__":
    main()