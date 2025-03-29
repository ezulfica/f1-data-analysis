import yaml
import os


def load_settings(path: str) -> dict: 
    """
    Loads configuration settings from a YAML file.

    Args:
        path (str): The path to the YAML file.

    Returns:
        dict: The parsed settings as a dictionary.
    """
        
    with open(path) as file:
        settings = yaml.safe_load(file)
    return settings


def get_all_file_paths(directory: str) -> list: 
    """
    Retrieves a list of all file paths within a given directory and its subdirectories.

    Args:
        directory (str): The root directory to search for files.

    Returns:
        list: A list of full file paths.
    """

    file_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths


# Example usage
all_files = get_all_file_paths("raw/")
