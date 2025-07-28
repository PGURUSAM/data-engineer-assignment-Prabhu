import os
from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env

import logging
from etl_pipeline.ETL_scripts.pipeline import etl_job
from etl_pipeline.ETL_scripts.utils import log_system_metrics
from etl_pipeline.scheduler import schedule_etl

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

if __name__ == "__main__":
    INPUT_PATH = os.getenv("ETL_INPUT")
    OUTPUT_PATH = os.getenv("ETL_OUTPUT")
    MODE = os.getenv("ETL_MODE")
    INTERVAL = int(os.getenv("ETL_INTERVAL_MINUTES", "60"))
    logging.info(f"Starting ETL pipeline in {MODE} mode")
    if MODE == "once":
        etl_job(INPUT_PATH, OUTPUT_PATH)
    else:
        schedule_etl(INPUT_PATH, OUTPUT_PATH, interval_minutes=INTERVAL)