import logging
from typing import Tuple, List, Dict
import pandas as pd
import os
import shutil

logger = logging.getLogger(__name__)

def validate_dataframe(df: pd.DataFrame, rules: Dict) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame against simple rules:
    - required_columns
    - max_null_fraction
    """
    errors: List[str] = []

    # 1) required columns
    required_cols = rules.get("required_columns", [])
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        errors.append(f"Missing columns: {missing}")

    # 2) null fraction
    max_null = rules.get("max_null_fraction", 0.5)
    total_cells = len(df) * len(df.columns) if len(df.columns) > 0 else 1
    null_fraction = df.isnull().sum().sum() / total_cells
    if null_fraction > max_null:
        errors.append(
            f"Too many nulls: {null_fraction:.2%} > allowed {max_null:.2%}"
        )

    if errors:
        logger.error("Validation failed: %s", errors)
        return False, errors

    logger.info("Validation passed with %d rows", len(df))
    return True, []


def move_to_error_folder(file_path: str, error_folder: str) -> None:
    """
    Move a file to the error folder for manual review.
    """
    if not os.path.exists(error_folder):
        os.makedirs(error_folder)
    
    file_name = os.path.basename(file_path)
    dest = os.path.join(error_folder, file_name)
    shutil.move(file_path, dest)
    logger.info(f"Moved {file_name} to {error_folder}")
