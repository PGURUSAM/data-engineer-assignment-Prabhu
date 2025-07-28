import os
import logging
from etl_pipeline.ETL_scripts.utils import log_system_metrics
from .extract import extract
from .transform import transform_energy_data
from .load import load
from .alert import send_alert_email

__version__ = "1.0.0"

def etl_job(input_path: str, output_path: str, local_tz: str = "Europe/Vilnius"):
    """Run the full ETL job: extract, transform, load."""
    try:
        logging.info(f"Running ETL version {__version__}")
        log_system_metrics()
        df = extract(input_path)
        df_transformed = transform_energy_data(df, local_tz)
        load(df_transformed, output_path)
        logging.info("ETL job completed successfully.")
    except Exception as e:
        logging.exception("ETL job failed")
        send_alert_email("ETL Job Failure", str(e))
        raise