import os
from typing import Dict


class DatabaseConfigError(Exception):
    pass


def load_db_config() -> Dict[str, str]:
    config = {
        "dbname": os.getenv("DB_NAME", "error"),
        "user": os.getenv("DB_USER", "error"),
        "password": os.getenv("DB_PASSWORD", "error"),
        "host": os.getenv("DB_HOST", "error"),
        "port": os.getenv("DB_PORT", "error"),
    }

    validate_db_config(config)

    return config


def validate_db_config(config: Dict[str, str]) -> None:
    dbname = config.get("dbname", "")

    # Check if using SQLite (filename ending with .db or .sqlite)
    if dbname.endswith((".db", ".sqlite", ".sqlite3")):
        if dbname == "error":
            raise DatabaseConfigError(
                "Configuration error: dbname is not set for SQLite"
            )
        # No need to validate user/password/host/port for SQLite
    else:
        # Normal (PostgreSQL or others): validate everything
        for key, value in config.items():
            if value == "error":
                raise DatabaseConfigError(f"Configuration error: {key} is not set")
