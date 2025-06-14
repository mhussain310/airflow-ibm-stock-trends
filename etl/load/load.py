import pandas as pd
import numpy as np
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    MetaData,
)
from sqlalchemy.exc import InternalError

from config.db_config import DatabaseConfigError, load_db_config
from config.file_path_config import BASE_OUTPUT_DIR
from utils.db_utils import (
    DatabaseConnectionError,
    QueryExecutionError,
    create_db_engine,
)
from utils.file_utils import (
    get_absolute_path,
    generate_data_file_path,
    save_dataframe_to_csv,
)


def _load_data(input_file_path: str, table_name: str):
    # Read CSV from file path
    data = pd.read_csv(get_absolute_path(input_file_path))

    # Load dataframe into a table in the database
    create_table(data, table_name)

    # Save loaded data as CSV files for logging purposes
    OUTPUT_DATA_FILE_PATH = generate_data_file_path(
        prefix=table_name, base_dir=BASE_OUTPUT_DIR
    )
    save_dataframe_to_csv(data, OUTPUT_DATA_FILE_PATH)

    return None


def create_table(data: pd.DataFrame, table_name: str):
    connection = None
    try:
        connection_details = load_db_config()
        engine = create_db_engine(connection_details)
        metadata = MetaData()

        # Copy and add 'id' as index
        df = data.copy()
        df["id"] = range(1, len(df) + 1)

        # Dynamically infer column types
        columns = [Column("id", Integer, primary_key=True)]
        for col in df.columns:
            if col != "id":
                col_type = map_dtype_to_sqlalchemy(df[col].dtype)
                columns.append(Column(col, col_type))

        # Define and create the table
        table = Table(table_name, metadata, *columns)
        metadata.drop_all(engine, [table], checkfirst=True)
        metadata.create_all(engine, [table])

        # Insert rows
        rows = df.to_dict(orient="records")
        with engine.begin() as conn:
            conn.execute(table.insert(), rows)

    except InternalError:
        print("Target table exists")
        raise
    except DatabaseConfigError as e:
        print(f"Target database not configured correctly: {e}")
        raise
    except DatabaseConnectionError as e:
        print(f"Failed to connect to the database when creating table:" f" {e}")
        raise
    except pd.errors.DatabaseError as e:
        print(f"Failed to create table: {e}")
        raise QueryExecutionError(f"Failed to execute query: {e}")
    finally:
        if connection is not None:
            connection.close()


def map_dtype_to_sqlalchemy(dtype):
    if np.issubdtype(dtype, np.integer):
        return Integer
    elif np.issubdtype(dtype, np.floating):
        return Float
    elif np.issubdtype(dtype, np.bool_):
        return Boolean
    elif np.issubdtype(dtype, np.datetime64):
        return DateTime
    else:
        return String  # default fallback
