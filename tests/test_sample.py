import os
from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env

import logging

import pandas as pd
import pytest

from etl_pipeline.ETL_scripts.utils import read_parquet_with_retries
from etl_pipeline.ETL_scripts.validation import compute_data_quality_score, validate_energy_consumption_column

def test_compute_data_quality_score():
    # Sample dataframe with all required columns
    df = pd.DataFrame({
        'client_id': ['A1', 'A2'],
        'date': ['2023-01-01', '2023-01-02'],
        'ext_dev_ref': ['dev1', 'dev2'],
        'energy_consumption': [[1, 2], [3, 4]],
        'resolution': ['hourly', '15min']
    })
    score = compute_data_quality_score(df)
    assert isinstance(score, (int, float))
    assert 0 <= score <= 100

def test_validate_energy_consumption_column():
    df = pd.DataFrame({
        'energy_consumption': ["[1, 2, 3]", [4, 5, 6]]
    })
    # This should convert string to list if the function handles it
    validate_energy_consumption_column(df)
    assert all(isinstance(x, list) for x in df['energy_consumption'])

def test_read_parquet_with_retries(tmp_path):
    df = pd.DataFrame({'a': [1, 2]})
    file_path = tmp_path / "test.parquet"
    df.to_parquet(file_path)

    df2 = read_parquet_with_retries(str(file_path))
    pd.testing.assert_frame_equal(df2, df)
