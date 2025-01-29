from dotenv import dotenv_values
import yaml 
import os 

def load_config(path):
    """Load environment variables from config file."""
    config = dotenv_values(path)
    return {
        "aws_access_key": config["AWS_ACCESS_KEY"],
        "aws_secret_key": config["AWS_SECRET_KEY"],
        "s3_bucket": config["s3_bucket"],
        "aws_region": config["aws_region"]
    }

def load_settings(path):
    with open(path) as file:
        settings = yaml.safe_load(file)
    return settings

def get_all_file_paths(directory):
    file_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

# Example usage
all_files = get_all_file_paths("raw/")

