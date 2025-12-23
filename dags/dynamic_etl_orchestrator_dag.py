import os
import glob
import logging
import yaml
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from pathlib import Path
import sys
sys.path.append("/opt/airflow/src")

from extract import read_csv
from validation import validate_dataframe, move_to_error_folder
from transform import transform_dataframe
from load import to_postgres, log_failed_load

logger = logging.getLogger(__name__)

CONFIG_PATH = "/opt/airflow/configs/pipelines.yaml"
LANDING_DIR = "/opt/airflow/data/landing"


def load_config():
    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("pipelines", [])


def find_latest_file(source_glob: str) -> str:
    files = glob.glob(source_glob)
    if not files:
        raise FileNotFoundError(f"No files found for pattern: {source_glob}")
    latest = max(files, key=os.path.getmtime)
    logger.info("Using latest file: %s", latest)
    return latest


def run_pipeline(pipeline: dict, **context):
    """
    Step-by-step pipeline for the online shop scenario:
    1. Extract (find latest file)
    2. Validate (check columns & nulls)
    3. Transform (clean types, add total_amount)
    4. Load (insert into sales table)
    5. Notify success
    """
    source_glob = pipeline["source_glob"]
    target_table = pipeline["target_table"]
    validation_rules = pipeline.get("validation", {})
    error_folder = pipeline.get("error_folder", "/opt/airflow/data/bad/")
    file_name = None
    
    try:
        # Step 1: Find latest matching file
        file_path = find_latest_file(source_glob)
        file_name = os.path.basename(file_path)
        logger.info("Step 1: Found new file %s", file_name)

        # Step 2: Extract
        df = read_csv(file_path)
        logger.info("Step 2: Extracted %d rows from %s", len(df), file_name)

        # Step 3: Validate
        is_valid, errors = validate_dataframe(df, validation_rules)
        logger.info("Step 3: Validation check - Valid: %s", is_valid)
        
        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error("Validation failed: %s. Moving to error folder.", error_msg)
            move_to_error_folder(file_path, error_folder)
            log_failed_load(file_name, error_msg)
            raise AirflowException(f"Validation failed for {file_name}: {error_msg}")

        # Step 4: Transform (clean + add total_amount)
        df_clean = transform_dataframe(df)
        logger.info("Step 4: Transformed %d rows (after cleaning)", len(df_clean))

        # Step 5: Load
        rows = to_postgres(df_clean, target_table)
        logger.info("Step 5: Loaded %d rows into %s", rows, target_table)

        # Step 6: Notify success
        success_msg = f"✓ Sales data for {file_name} loaded successfully, {rows} orders processed."
        logger.info("Step 6: SUCCESS - %s", success_msg)
        
    except Exception as e:
        # Step 7: Send error message
        error_msg = str(e)
        if file_name:
            log_failed_load(file_name, error_msg)
            logger.error("Step 7: ERROR - Failed to process %s: %s", file_name, error_msg)
        raise


# --- Define the DAG ---

default_args = {
    "owner": "shop_owner",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="dynamic_etl_orchestrator",
    default_args=default_args,
    schedule_interval=None,  # trigger manually for now
    catchup=False,
    description="Dynamic ETL Orchestrator for Online Shop - Auto-processes daily sales files",
    tags=["etl", "shop", "sales"],
) as dag:

    pipelines = load_config()

    for p in pipelines:
        if not p.get("enabled", True):
            continue

        task_id = f"run_pipeline__{p['name']}"

        PythonOperator(
            task_id=task_id,
            python_callable=run_pipeline,
            op_kwargs={"pipeline": p},
            doc=f"Process {p.get('description', p['name'])}",
        )
