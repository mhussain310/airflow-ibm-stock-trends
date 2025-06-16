import pandas as pd

from config.file_path_config import BASE_PROCESSED_DIR
from etl.transform.date_transformations import floor_date_to_hour
from utils.file_utils import (
    get_absolute_path,
    generate_data_file_path,
    save_dataframe_to_csv,
)


def _enrich_stock_data(input_file_path: str, output_file_name: str, column: str) -> str:
    # Read CSV from file path
    stock_data = pd.read_csv(get_absolute_path(input_file_path))

    # Transform the date column
    stock_data = floor_date_to_hour(stock_data, column=column)

    # Save the dataframe as a CSV for logging purposes
    PROCESSED_DATA_FILE_PATH = generate_data_file_path(
        prefix=output_file_name, base_dir=BASE_PROCESSED_DIR, subdir="enriched/stock"
    )
    save_dataframe_to_csv(stock_data, PROCESSED_DATA_FILE_PATH)

    # Return the filepath of the enriched csv
    return PROCESSED_DATA_FILE_PATH
