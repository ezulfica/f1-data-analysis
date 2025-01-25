from utils.s3_utils import connect_s3
from utils.config_utils import load_config, load_settings
from data_prep.src.utils import build_object_list_to_query
from data_prep.src.utils import data_loading_concurrency

SETTINGS_PATH = "config/settings.yaml"
CONFIG_PATH = "config/.env"


def main() : 
    settings = load_settings(SETTINGS_PATH)
    config = load_config(CONFIG_PATH)
    s3_client = connect_s3(config)
    s3_files = s3_client.list_objects()
    season_category = settings["f1_api"]["season_category"]
    #round_category = settings["f1_api"]["round_category"]
    season_category_files = build_object_list_to_query(season_category, s3_files)
    data_loading_concurrency(s3_client, files=season_category_files)


if __name__ == "__main__":
    main()


