import os
import logging
import time
import pandas as pd
from .utils import read_parquet_with_retries
from .validation import validate_schema
from .alert import send_alert_email

def extract(input_path: str) -> pd.DataFrame:
    """Extract raw data from parquet file with retries and validate schema only."""
    t0 = time.time()
    logging.info(f"[extract] Starting extraction from {input_path}")
    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        df = read_parquet_with_retries(input_path)
        logging.info(f"[extract] Parquet file read successfully")
        validate_schema(df)
        logging.info(f"[extract] Schema validation passed")
        t1 = time.time()
        logging.info(f"[extract] Extraction completed in {t1-t0:.2f}s")
        return df
    except Exception as e:
        logging.exception(f"[extract] Error: {str(e)}")
        send_alert_email("ETL Extract Failure", str(e))
        raise