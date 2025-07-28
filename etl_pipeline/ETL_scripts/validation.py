import logging
import pandas as pd
import numpy as np
import ast
import re

def validate_schema(df: pd.DataFrame):
    """Validate that required columns exist in the DataFrame."""
    expected_columns = ['client_id', 'date', 'ext_dev_ref', 'energy_consumption', 'resolution']
    for col in expected_columns:
        if col not in df.columns:
            raise ValueError(f"Missing expected column: {col}")
    logging.info("Schema validation passed.")

def validate_id_format(value: str):
    """Validate client and device ID format."""
    if not re.match(r'^[A-Za-z0-9_]+$', value):
        raise ValueError(f"Invalid ID format: {value}")

def validate_energy_consumption_column(df: pd.DataFrame, invalid_rows_path: str = None):
    """Ensure energy_consumption column contains lists. Drop and log invalid rows."""
    def to_list(x):
        if isinstance(x, list):
            return x
        if isinstance(x, str):
            try:
                val = ast.literal_eval(x)
                return val if isinstance(val, list) else np.nan
            except Exception:
                return np.nan
        if isinstance(x, np.ndarray):
            return x.tolist()
        return np.nan
    df['energy_consumption'] = df['energy_consumption'].apply(to_list)
    invalid_rows = df[df['energy_consumption'].isna()]
    if not invalid_rows.empty:
        logging.warning(f"Dropping {len(invalid_rows)} rows with invalid energy_consumption format.")
        if invalid_rows_path:
            invalid_rows.to_parquet(invalid_rows_path, index=False)
            logging.info(f"Invalid rows saved to {invalid_rows_path}")
        df.drop(index=invalid_rows.index, inplace=True)

def check_missing_values(df: pd.DataFrame):
    """Log missing values in DataFrame."""
    missing = df.isnull().sum()
    if missing.any():
        logging.warning(f"Missing values detected:\n{missing}")
    else:
        logging.info("No missing values detected.")

def compute_data_quality_score(df: pd.DataFrame) -> float:
    """Compute a simple data quality score (0-100)."""
    expected_columns = ['client_id', 'date', 'ext_dev_ref', 'energy_consumption', 'resolution']
    total_expected = len(expected_columns)
    present_cols = sum(1 for c in expected_columns if c in df.columns)
    schema_score = (present_cols / total_expected) * 50
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    completeness_score = 0
    if total_cells > 0:
        completeness_score = 50 * (1 - missing_cells / total_cells)
    else:
        logging.warning("DataFrame is empty. Completeness score set to 0.")
    score = schema_score + completeness_score
    logging.info(f"Data Quality Score: {score:.2f}/100")
    return score