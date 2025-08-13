import yfinance as yf
import pandas as pd
import os
import logging
import dotenv
from src.db_connection import get_snowflake_connection
from dotenv import load_dotenv
load_dotenv()

TABLE_NAME = "BENCHMARKGENERALINFO"

def fetch_benchmark_metadata(tickers):
    """
    Fetch benchmark metadata from Yahoo Finance for each ticker.
    """
    records = []
    for symbol in tickers:
        try:
            info = yf.Ticker(symbol).info
            name = info.get("longName") or info.get("shortName") or symbol
            benchmark_type = "Index" if "index" in name.lower() else "ETF"
            region = info.get("region", "Global") or "Global"

            records.append({
                "BENCHMARKCODE": symbol,
                "BENCHMARKNAME": name,
                "BENCHMARKTYPE": benchmark_type,
                "PROVIDER": "Yahoo Finance",
                "REGION": region
            })
        except Exception as e:
            print(f"Failed to fetch {symbol}: {e}")
    return pd.DataFrame(records)

def insert_benchmark_data(conn, df):
    """
    Insert benchmark records into Snowflake.
    """
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        BENCHMARKCODE, BENCHMARKNAME, BENCHMARKTYPE, PROVIDER, REGION
    )
    VALUES (%s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_sql, (
                row['BENCHMARKCODE'],
                row['BENCHMARKNAME'],
                row['BENCHMARKTYPE'],
                row['PROVIDER'],
                row['REGION']
            ))
    conn.commit()
    print(f"Inserted {len(df)} rows into {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()

    yfinance_benchmarks = [
        "SPY", "QQQ", "DIA", "IWM", "VTI", "EFA", "EEM", "AGG", "LQD", "HYG",
        "GLD", "VNQ", "TLT", "IVV", "VWO", "BND", "SHV", "SCHF", "IEFA", "XLF"
    ]

    df = fetch_benchmark_metadata(yfinance_benchmarks)
    insert_benchmark_data(conn, df)
    conn.close()
