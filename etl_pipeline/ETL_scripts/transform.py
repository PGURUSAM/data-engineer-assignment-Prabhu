import logging
import time
import pandas as pd
from .utils import convert_dtypes, local_to_utc
from .validation import (
    validate_id_format,
    validate_energy_consumption_column,
    check_missing_values,
    compute_data_quality_score,
)
from .feature_engineering import feature_engineering_and_aggregation
from .alert import send_alert_email

def transform_energy_data(df: pd.DataFrame, local_tz: str = "Europe/Vilnius") -> pd.DataFrame:
    """Transform and clean the DataFrame."""
    t0 = time.time()
    logging.info("[transform] Starting transformation")
    try:
        df = convert_dtypes(df)
        logging.info("[transform] Data types converted")
        df = local_to_utc(df, 'date', local_tz)
        logging.info("[transform] Date column converted to UTC")
        df['client_id'].apply(validate_id_format)
        df['ext_dev_ref'].apply(validate_id_format)
        logging.info("[transform] ID formats validated")
        invalid_rows_path = "invalid_energy_rows.parquet"
        validate_energy_consumption_column(df, invalid_rows_path=invalid_rows_path)
        logging.info("[transform] Energy consumption column validated")
        check_missing_values(df)
        logging.info("[transform] Missing values checked")
        compute_data_quality_score(df)
        logging.info("[transform] Data quality score computed")
        df_transformed = feature_engineering_and_aggregation(df, local_tz)
        t1 = time.time()
        logging.info(f"[transform] Transformation completed in {t1-t0:.2f}s")
        return df_transformed
    except Exception as e:
        logging.exception(f"[transform] Error: {str(e)}")
        send_alert_email("ETL Transform Failure", str(e))
        raise