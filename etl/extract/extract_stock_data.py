import os
from typing import List

import pandas as pd

from config.api_config import get_stock_api_key
from config.file_path_config import BASE_RAW_DIR
from utils.file_utils import generate_data_file_path, get_absolute_path
from utils.request_utils import get_url

# Uncomment the below 2 for testing purposes and not to overwhelm API.

# hourly_stock_data_url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo&datatype=csv"
# daily_stock_data_url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo&datatype=csv"

# Comment the above 2 and Uncomment the below 2 for production

hourly_stock_data_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=60min&outputsize=full&apikey={get_stock_api_key()}&datatype=csv"
daily_stock_data_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo&datatype=csv"


def _extract_stock_data() -> List[pd.DataFrame]:
    try:
        HOURLY_STOCK_DATA_FILE_PATH = generate_data_file_path(
            prefix="hourly_stock_data", base_dir=BASE_RAW_DIR, subdir="stock"
        )
        DAILY_STOCK_DATA_FILE_PATH = generate_data_file_path(
            prefix="daily_stock_data", base_dir=BASE_RAW_DIR, subdir="stock"
        )

        fetch_stock_data(hourly_stock_data_url, HOURLY_STOCK_DATA_FILE_PATH)
        fetch_stock_data(daily_stock_data_url, DAILY_STOCK_DATA_FILE_PATH)

        return {
            "hourly": HOURLY_STOCK_DATA_FILE_PATH,
            "daily": DAILY_STOCK_DATA_FILE_PATH,
        }
    except Exception as e:
        raise Exception(f"Failed to extract data: {e}")


def fetch_stock_data(url: str, file_path: str) -> pd.DataFrame:
    # Get the absolute file path
    absolute_file_path = get_absolute_path(file_path)

    # Ensure the directory exists
    os.makedirs(os.path.dirname(absolute_file_path), exist_ok=True)

    # Fetch the stock data
    response = get_url(url)

    # Write the content of the response to the file path created
    with open(absolute_file_path, "wb") as file:
        file.write(response.content)


if __name__ == "__main__":
    _extract_stock_data()
