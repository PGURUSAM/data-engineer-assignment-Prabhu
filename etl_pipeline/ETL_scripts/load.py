import logging
import time
from .utils import write_parquet_with_retries
from .alert import send_alert_email

def load(df, output_path):
    """Load transformed DataFrame to parquet file with retries."""
    t0 = time.time()
    logging.info(f"[load] Starting load to {output_path}")
    try:
        write_parquet_with_retries(df, output_path)
        t1 = time.time()
        logging.info(f"[load] Load completed in {t1-t0:.2f}s")
    except Exception as e:
        logging.exception(f"[load] Error: {str(e)}")
        send_alert_email("ETL Load Failure", str(e))
        raise