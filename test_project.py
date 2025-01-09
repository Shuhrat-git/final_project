#!/usr/bin/env python
"""
test_project.py
---------------
Pytest-based tests for the three required functions in project.py:
    1) test_fetch_data()
    2) test_store_data_in_sql()
    3) test_compute_signals()
"""

import pytest
import os
import sqlite3
import pandas as pd
from project import fetch_data, store_data_in_sql, compute_signals

TEST_DB_NAME = "test_crypto.db"
TEST_TABLE_NAME = "test_btc_prices"


@pytest.fixture
def cleanup_test_db():
    """
    A pytest fixture to remove the test database file after each test run
    to ensure a clean state.
    """
    yield
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)


def test_fetch_data():
    """
    Tests that fetch_data returns a DataFrame with the expected columns
    and is not empty (assuming Binance returns data).
    """
    df = fetch_data()
    expected_cols = ["timestamp", "open", "high", "low", "close", "volume"]
    for col in expected_cols:
        assert col in df.columns, f"Missing column: {col}"
    # We expect at least 1 row (assuming the API call succeeds)
    assert not df.empty, "DataFrame is empty, expected some rows."


def test_store_data_in_sql(cleanup_test_db):
    """
    Tests that store_data_in_sql creates the table and inserts data.
    """
    # Create a small df
    data = {
        "timestamp": pd.date_range("2023-01-01", periods=5, freq="D"),
        "open": [17000, 17100, 17200, 17300, 17400],
        "high": [17100, 17200, 17300, 17400, 17500],
        "low": [16900, 17000, 17100, 17200, 17300],
        "close": [17050, 17150, 17250, 17350, 17450],
        "volume": [100, 200, 300, 400, 500],
    }
    df = pd.DataFrame(data)

    rows_inserted = store_data_in_sql(df, db_name=TEST_DB_NAME, table_name=TEST_TABLE_NAME)
    assert rows_inserted == 5, "Expected to insert 5 rows."

    # Check if data actually got inserted
    conn = sqlite3.connect(TEST_DB_NAME)
    df_db = pd.read_sql_query(f"SELECT * FROM {TEST_TABLE_NAME}", conn)
    conn.close()

    assert len(df_db) == 5, "Expected 5 rows in the test table."
    # Check columns exist
    for col in ["timestamp", "open", "high", "low", "close", "volume"]:
        assert col in df_db.columns, f"Column {col} not found in DB"


def test_compute_signals(cleanup_test_db):
    """
    Tests that compute_signals successfully reads data from SQL,
    computes indicators, and returns a DataFrame with a 'signal' column.
    """
    # Insert minimal data into test DB
    data = {
        "timestamp": pd.date_range("2023-01-01", periods=10, freq="D"),
        "open": [17000, 17100, 17150, 17200, 17300, 17250, 17280, 17310, 17400, 17500],
        "high": [17050, 17150, 17200, 17250, 17350, 17300, 17330, 17360, 17450, 17550],
        "low": [16950, 17050, 17100, 17150, 17250, 17200, 17230, 17260, 17350, 17450],
        "close": [17020, 17120, 17180, 17220, 17310, 17270, 17290, 17320, 17410, 17510],
        "volume": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    }
    df_test = pd.DataFrame(data)
    store_data_in_sql(df_test, db_name=TEST_DB_NAME, table_name=TEST_TABLE_NAME)

    # Now compute signals
    df_signals = compute_signals(db_name=TEST_DB_NAME, table_name=TEST_TABLE_NAME)

    # We expect some columns like EMA_20, RSI, VWAP, BB_upper, BB_lower, signal
    for col in ["EMA_20", "RSI", "VWAP", "BB_upper", "BB_lower", "signal"]:
        assert col in df_signals.columns, f"Missing indicator column: {col}"

    # Ensure 'signal' column is present and not all NaN
    assert "signal" in df_signals.columns, "No 'signal' column found"
    assert not df_signals["signal"].isna().all(), "Signal column is entirely NaN"


"""
Usage:
------
1) Install required libraries:
    pip install pytest ccxt pandas pandas_ta

2) From the project root folder, run:
    pytest
"""
