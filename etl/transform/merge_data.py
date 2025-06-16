import pandas as pd

from config.file_path_config import BASE_PROCESSED_DIR
from utils.file_utils import (
    get_absolute_path,
    generate_data_file_path,
    save_dataframe_to_csv,
)


def _merge_data(
    input_file_path_1: str,
    input_file_path_2: str,
    left_on: str,
    right_on: str,
    output_file_name: str,
    how="inner",
) -> str:
    # Read CSV from file path
    df1 = pd.read_csv(get_absolute_path(input_file_path_1))
    df2 = pd.read_csv(get_absolute_path(input_file_path_2))

    # Merge the DataFrames
    merged_df = pd.merge(
        df1,
        df2,
        left_on=left_on,
        right_on=right_on,
        how=how,
    )

    # Drop one of the duplicate datetime columns
    merged_df.drop(columns=[right_on], inplace=True)

    # Save the dataframe as a CSV for logging purposes
    PROCESSED_DATA_FILE_PATH = generate_data_file_path(
        prefix=output_file_name, base_dir=BASE_PROCESSED_DIR, subdir="merged"
    )
    save_dataframe_to_csv(merged_df, PROCESSED_DATA_FILE_PATH)

    # Return the filepath of the merged csv
    return PROCESSED_DATA_FILE_PATH
