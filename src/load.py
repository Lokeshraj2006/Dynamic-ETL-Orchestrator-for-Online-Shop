import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow_db"


def to_postgres(df: pd.DataFrame, table_name: str, db_url: Optional[str] = None) -> int:
    """
    Append DataFrame rows into a Postgres table.
    """
    if db_url is None:
        db_url = DB_URL

    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="append", index=False)
    row_count = len(df)
    logger.info("Loaded %d rows into %s", row_count, table_name)
    return row_count


def log_failed_load(file_name: str, error_message: str, db_url: Optional[str] = None) -> None:
    """
    Log a failed file load to the failed_loads table (Step 7: send error message).
    """
    if db_url is None:
        db_url = DB_URL
    
    engine = create_engine(db_url)
    error_df = pd.DataFrame({
        "file_name": [file_name],
        "error_message": [error_message]
    })
    error_df.to_sql("failed_loads", engine, if_exists="append", index=False)
    logger.info("Logged error for %s", file_name)
