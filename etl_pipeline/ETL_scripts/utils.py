import logging
import time
import psutil
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def log_system_metrics():
    """Log system CPU and memory usage."""
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory().percent
    logging.info(f"System usage: CPU {cpu}%, Memory {mem}%")

def read_parquet_with_retries(file_path: str, retries: int = 3, delay: int = 2) -> pd.DataFrame:
    """Read parquet file with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempt {attempt}: Reading parquet {file_path}")
            return pd.read_parquet(file_path)
        except Exception as e:
            logging.error(f"Read error: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise

def write_parquet_with_retries(df: pd.DataFrame, output_path: str, retries: int = 3, delay: int = 2):
    """Write DataFrame to parquet file with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempt {attempt}: Writing parquet {output_path}")
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)
            logging.info(f"[load] Output written to {output_path}")
            return
        except Exception as e:
            logging.error(f"Write error: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise

def resolution_to_minutes(res_str: str) -> int:
    """Convert resolution string to minutes."""
    import re
    if not isinstance(res_str, str):
        return 60
    s = res_str.lower().strip()
    num_match = re.search(r"(\d+)", s)
    num = int(num_match.group(1)) if num_match else None
    if "month" in s:
        return 43200
    elif "day" in s:
        return 1440
    elif "hour" in s or "hr" in s or "1h" in s:
        return (num * 60) if num else 60
    elif "min" in s:
        return num if num else 60
    else:
        return 60

def local_to_utc(df: pd.DataFrame, column_name: str, local_tz: str = "Europe/Vilnius") -> pd.DataFrame:
    """Convert local time to UTC for a given column."""
    import pytz
    tz = pytz.timezone(local_tz)
    df[column_name] = pd.to_datetime(df[column_name], errors="raise")
    def convert_to_utc(dt):
        if dt.tzinfo is None:
            return tz.localize(dt, is_dst=None).astimezone(pytz.UTC)
        else:
            return dt.astimezone(pytz.UTC)
    df[column_name] = df[column_name].apply(convert_to_utc)
    return df

def utc_to_local(df: pd.DataFrame, column_name: str, local_tz: str = "Europe/Vilnius") -> pd.DataFrame:
    """Convert UTC time to local timezone for a given column."""
    import pytz
    tz = pytz.timezone(local_tz)
    df[column_name] = pd.to_datetime(df[column_name], utc=True).dt.tz_convert(tz)
    return df

def convert_dtypes(df: pd.DataFrame):
    """Convert columns to appropriate data types."""
    df['client_id'] = df['client_id'].astype(str)
    df['ext_dev_ref'] = df['ext_dev_ref'].astype(str)
    df['date'] = pd.to_datetime(df['date'])
    return df