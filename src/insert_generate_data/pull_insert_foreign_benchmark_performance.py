# Alpha Vantage Source

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from src.db_connection import get_snowflake_connection

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"
TABLE_NAME = "BENCHMARKPERFORMANCE"

def fetch_foreign_index(symbol: str, outputsize: str = "full") -> pd.DataFrame:
    """
    Fetch daily prices for a foreign index or stock using Alpha Vantage API.
    Returns a DataFrame with benchmark performance data.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": outputsize,
        "apikey": API_KEY
    }
    r = requests.get(BASE_URL, params=params)
    data = r.json()

    if "Time Series (Daily)" not in data:
        print(f"{symbol} not available or restricted: {data.get('Note', data)}")
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.rename(columns={
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    })

    df["BENCHMARKCODE"] = symbol
    df["PERFORMANCEDATATYPE"] = "Prices"
    df["CURRENCYCODE"] = "USD"  # Adjust if needed
    df["PERFORMANCEFREQUENCY"] = "Daily"
    df["HISTORYDATE"] = df.index
    df["VALUE"] = pd.to_numeric(df["close"], errors="coerce")

    return df[[
        "BENCHMARKCODE", "PERFORMANCEDATATYPE", "CURRENCYCODE",
        "PERFORMANCEFREQUENCY", "HISTORYDATE", "VALUE"
    ]].reset_index(drop=True)

def validate_benchmark_data(df: pd.DataFrame):
    """
    Validate benchmark performance data before inserting.
    Checks for missing values, duplicates, and invalid ranges.
    """
    issues = []

    if df.empty:
        issues.append("DataFrame is empty.")

    required_cols = [
        "BENCHMARKCODE", "PERFORMANCEDATATYPE", "CURRENCYCODE",
        "PERFORMANCEFREQUENCY", "HISTORYDATE", "VALUE"
    ]
    for col in required_cols:
        if col not in df.columns:
            issues.append(f"Missing required column: {col}")

    if df["VALUE"].isna().any():
        issues.append("Some VALUE entries are missing or non-numeric.")

    if (df["VALUE"] < 0).any():
        issues.append("Negative values detected in VALUE column.")

    # Convert HISTORYDATE to Python date to avoid Snowflake timestamp error
    df["HISTORYDATE"] = pd.to_datetime(df["HISTORYDATE"], errors="coerce").dt.date

    # Remove duplicates
    df.drop_duplicates(subset=["BENCHMARKCODE", "HISTORYDATE"], inplace=True)

    return issues, df

def insert_benchmark_performance(conn, df: pd.DataFrame):
    """
    Insert validated benchmark performance data into Snowflake.
    """
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        BENCHMARKCODE, PERFORMANCEDATATYPE, CURRENCYCODE,
        PERFORMANCEFREQUENCY, HISTORYDATE, VALUE
    ) VALUES (%s, %s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        cur.executemany(insert_sql, df.values.tolist())
    conn.commit()
    print(f"Inserted {len(df)} rows into {TABLE_NAME}.")

if __name__ == "__main__":
    tickers = ["SONY", "TSM", "BABA", "SAP", "SHOP", "TM"] # Let's assume some foreign stocks/tickers as benchmarks in this example
    all_data = []

    for ticker in tickers:
        print(f"Fetching {ticker}...")
        df = fetch_foreign_index(ticker)
        if not df.empty:
            issues, validated_df = validate_benchmark_data(df)
            if issues:
                print(f"Issues for {ticker}:")
                for issue in issues:
                    print(f" - {issue}")
            else:
                all_data.append(validated_df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        conn = get_snowflake_connection()
        insert_benchmark_performance(conn, final_df)
        conn.close()
