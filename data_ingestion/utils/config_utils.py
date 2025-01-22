from dotenv import dotenv_values

def load_config(path):
    """Load environment variables from config file."""
    config = dotenv_values(path)
    return {
        "aws_access_key": config["AWS_ACCESS_KEY"],
        "aws_secret_key": config["AWS_SECRET_KEY"],
        "s3_bucket": config["s3_bucket"],
        "s3_prefix_folder": config["s3_prefix_folder"],
        "aws_region": config["aws_region"],
    }