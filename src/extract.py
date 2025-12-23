import pandas as pd
import logging

logger = logging.getLogger(__name__)

def read_csv(file_path: str) -> pd.DataFrame:
    """
    Read CSV file and return as DataFrame.
    """
    df = pd.read_csv(file_path)
    logger.info("Extracted %d rows from %s", len(df), file_path)
    return df
