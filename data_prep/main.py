from utils.s3_utils import connect_s3
from utils.config_utils import load_config, load_settings
from data_prep.src.utils import build_object_list_to_query
from data_prep.src.utils import data_loading_concurrency

SETTINGS_PATH = "config/settings.yaml"
CONFIG_PATH = "config/.env"


def main() : 
    config = load_config(CONFIG_PATH)
    s3_client = connect_s3(config)
    s3_files = s3_client.list_objects(prefix="raw/")
    last_upload = max([file for file in s3_files if "raw/" in file and ".txt" in file])
    file_to_prep = s3_client.read_object(last_upload).decode().split("\n")
    data_loading_concurrency(s3_client=s3_client, files = file_to_prep)

if __name__ == "__main__":
    main()

file_to_prep = s3_client.read_object(last_upload).decode().split("\n")
file_to_prep = [file for file in file_to_prep if "2024" in file]