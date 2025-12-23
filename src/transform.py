import logging
import pandas as pd

logger = logging.getLogger(__name__)


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich DataFrame for the shop scenario.
    - Fix types
    - Calculate total_amount = quantity * price
    - Drop bad rows
    """
    df = df.copy()

    # Convert order_date to datetime
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Convert numeric columns
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Calculate total_amount for reporting (Step 4: helpful information)
    if "quantity" in df.columns and "price" in df.columns:
        df["total_amount"] = df["quantity"] * df["price"]
        logger.info("Calculated total_amount = quantity * price")

    # Drop rows with null in critical columns
    critical_cols = [c for c in ["order_id", "quantity", "price"] if c in df.columns]
    if critical_cols:
        before = len(df)
        df = df.dropna(subset=critical_cols)
        logger.info("Dropped %d rows with nulls in %s", before - len(df), critical_cols)

    return df
