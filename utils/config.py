import yaml
import os


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
