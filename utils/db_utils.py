import os

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from utils.file_utils import get_absolute_path


class DatabaseConnectionError(Exception):
    pass


class QueryExecutionError(Exception):
    pass


def get_db_connection(connection_params):
    try:
        engine = create_db_engine(connection_params)
        connection = engine.connect()
        return connection
    except OperationalError as e:
        raise DatabaseConnectionError(
            f"Operational error when connecting to the database: {e}"
        )
    except SQLAlchemyError as e:
        raise DatabaseConnectionError(f"Failed to connect to the database: {e}")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")


def create_db_engine(connection_params):
    try:
        dbname = connection_params.get("dbname")

        if not dbname:
            raise ValueError("Database name is missing")

        if dbname.endswith((".db", ".sqlite", ".sqlite3")):
            # Development (SQLite)
            db_path = os.path.normpath(get_absolute_path(dbname))

            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

            engine = create_engine(f"sqlite:///{db_path}")
        else:
            # Production (Postgres)
            required_keys = ["user", "password", "host", "port"]
            for key in required_keys:
                if not connection_params.get(key):
                    raise ValueError(f"Missing required DB connection parameter: {key}")

            engine = create_engine(
                f"postgresql+psycopg2://{connection_params['user']}"
                f":{connection_params['password']}@{connection_params['host']}"
                f":{connection_params['port']}/{connection_params['dbname']}"
            )

        return engine

    except ValueError as e:
        raise DatabaseConnectionError(f"Invalid Connection Parameters: {e}")
