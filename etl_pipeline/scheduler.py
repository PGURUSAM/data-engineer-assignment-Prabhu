import os
from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env

import schedule
import time
import logging
from etl_pipeline.ETL_scripts.pipeline import etl_job

def schedule_etl(input_path, output_path, interval_minutes=60):
    """Schedule ETL job to run at a fixed interval."""
    schedule.every(interval_minutes).minutes.do(etl_job, input_path, output_path)
    logging.info(f"Scheduled ETL every {interval_minutes} minutes")
    while True:
        schedule.run_pending()
        time.sleep(10)