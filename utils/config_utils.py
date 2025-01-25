from dotenv import dotenv_values
import yaml 

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