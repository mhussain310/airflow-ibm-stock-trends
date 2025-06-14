import os
import shutil
from datetime import datetime

import pandas as pd


def get_absolute_path(relative_path: str, base_dir: str = None) -> str:
    """
    Get the absolute file path from a relative path.

    :param relative_path: The relative path to the file.
    :param base_dir: The base directory to resolve the relative path from (defaults to the root of the project).
    :return: Absolute file path.
    """

    if base_dir is None:
        base_dir = find_project_root()

    return os.path.join(base_dir, relative_path)


def find_project_root(marker_file: str = "README.md") -> str:
    """
    Find the root directory of the project by looking for a marker file.

    :param marker_file: The name of the marker file to look for.
    :return: The absolute path to the root directory of the project.
    """
    current_dir = os.path.abspath(os.path.dirname(__file__))
    while current_dir != os.path.dirname(current_dir):
        if marker_file in os.listdir(current_dir):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    raise FileNotFoundError(
        f"Marker file '{marker_file}' not found in any parent directories."
    )


def save_dataframe_to_csv(
    df: pd.DataFrame, relative_path_to_file: str, base_dir: str = None
):
    """
    Save a pandas DataFrame to a CSV file.

    :param df (pd.DataFrame): The DataFrame to save.
    :param relative_path_to_file (str): The relative path to save the file to.
    """
    absolute_csv_file_path = get_absolute_path(relative_path_to_file, base_dir)
    os.makedirs(os.path.dirname(absolute_csv_file_path), exist_ok=True)
    df.to_csv(absolute_csv_file_path, index=False)


def generate_data_file_path(
    prefix: str, base_dir: str, subdir: str = "", ts: datetime = None, ext: str = "csv"
) -> str:
    """
    Generate a timestamped file path for any stage (raw, processed, output).

    Args:
        prefix (str): Filename prefix without extension (e.g., 'cleaned_current_weather')
        base_dir (str): Base directory (e.g., 'data/raw', 'data/processed')
        subdir (str): Optional subdirectory within base_dir (e.g., 'cleaned/weather')
        ts (datetime, optional): Timestamp to include in filename. Defaults to now.
        ext (str): File extension (default 'csv')

    Returns:
        str: Full file path
    """
    ts = ts or datetime.now()
    filename = f"{prefix}_{ts:%Y-%m-%d_%H%M%S}.{ext}"
    full_path = (
        f"{base_dir}/{subdir}/{filename}" if subdir else f"{base_dir}/{filename}"
    )
    return str(full_path)


def _clear_data_folders(base_dir="data"):
    """
    Deletes all files and subdirectories inside 'raw', 'processed', and 'output'
    subfolders of the given base_dir.

    Parameters:
        base_dir (str): Path to the base data directory. Default is 'data'.
    """
    subfolders = ["raw", "processed", "output"]

    for folder in subfolders:
        folder_path = get_absolute_path(f"{base_dir}/{folder}")
        if not os.path.exists(folder_path):
            print(f"Skipping missing folder: {folder_path}")
            continue

        # Iterate over all items inside the folder
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            try:
                if os.path.isfile(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                print(f"Error deleting {item_path}: {e}")
