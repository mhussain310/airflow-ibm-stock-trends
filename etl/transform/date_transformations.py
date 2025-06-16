import pandas as pd


def format_date(df: pd.DataFrame, column: str = "time") -> pd.DataFrame:
    # Convert 'time' column to datetime format
    df[column] = pd.to_datetime(df[column])

    # Format it to 'YYYY-MM-DD HH:MM:SS'
    df[column] = df[column].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert back to object(string)
    df[column] = df[column].astype(str)

    return df


def floor_date_to_hour(df: pd.DataFrame, column: str = "timestamp") -> pd.DataFrame:
    # Convert 'timestamp' column to datetime format
    df[column] = pd.to_datetime(df[column])

    # Floor to nearest hour
    df[column] = df[column].dt.floor("h")

    # Drop duplicates, keeping the first occurrence
    df.drop_duplicates(subset=[column], keep="first", inplace=True, ignore_index=True)

    # Convert back to object(string)
    df[column] = df[column].astype(str)

    return df
