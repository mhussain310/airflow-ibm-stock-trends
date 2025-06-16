import pandas as pd

from config.file_path_config import BASE_PROCESSED_DIR
from utils.file_utils import (
    get_absolute_path,
    generate_data_file_path,
    save_dataframe_to_csv,
)


def _clean_stock_data(
    input_file_path: str, output_file_name: str, sort_by: str, has_time: bool = False
) -> str:
    # Read CSV from file path
    stock_data = pd.read_csv(get_absolute_path(input_file_path))

    # Convert dictionary columns to strings (if any)
    for column in stock_data.columns:
        if isinstance(
            stock_data[column].iloc[0], dict
        ):  # Check if the column contains dictionaries
            stock_data[column] = stock_data[column].apply(str)  # Convert to string

    # Convert date/time column to datetime
    if sort_by in stock_data.columns:
        stock_data[sort_by] = pd.to_datetime(stock_data[sort_by], errors="coerce")

    # Sort values
    stock_data = sort_values(stock_data, sort_by)

    # Convert datetime back to original string format
    str_format = "%Y-%m-%d %H:%M:%S" if has_time else "%Y-%m-%d"
    stock_data[sort_by] = stock_data[sort_by].dt.strftime(str_format)

    # Remove duplicates
    stock_data = stock_data.drop_duplicates()

    # Save the dataframe as a CSV for logging purposes
    PROCESSED_DATA_FILE_PATH = generate_data_file_path(
        prefix=output_file_name, base_dir=BASE_PROCESSED_DIR, subdir="cleaned/stock"
    )
    save_dataframe_to_csv(stock_data, PROCESSED_DATA_FILE_PATH)

    # Return the filepath of the cleaned csv
    return PROCESSED_DATA_FILE_PATH


def sort_values(weather_data: pd.DataFrame, sort_by: str) -> pd.DataFrame:
    weather_data = weather_data.sort_values(by=sort_by, ascending=True)
    return weather_data
