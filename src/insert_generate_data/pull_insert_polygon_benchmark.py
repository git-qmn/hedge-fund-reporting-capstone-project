# Polygon source
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from src.db_connection import get_snowflake_connection

load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io"
TABLE_NAME = "BENCHMARKPERFORMANCE"

def fetch_benchmark_full_history(ticker, from_date, to_date, column_to_use="close"):
    all_results = []
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "apiKey": POLYGON_API_KEY
    }
    next_url = None

    while True:
        req_url = url if next_url is None else f"{BASE_URL}{next_url}"
        resp = requests.get(req_url, params=params if next_url is None else None)
        data = resp.json()
        if "results" not in data:
            print(f"Error or no more data for {ticker}: {data}")
            break
        all_results.extend(data["results"])
        if "next_url" in data and data["next_url"]:
            next_url = data["next_url"]
        else:
            break

    if not all_results:
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    df["HISTORYDATE"] = pd.to_datetime(df["t"], unit="ms").dt.date  # convert to Python date
    df = df.rename(columns={
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume"
    })

    df["BENCHMARKCODE"] = ticker.upper()
    df["PERFORMANCEDATATYPE"] = "Prices"
    df["CURRENCYCODE"] = "USD"
    df["PERFORMANCEFREQUENCY"] = "Daily"
    df["VALUE"] = pd.to_numeric(df[column_to_use], errors="coerce")

    return df[[
        "BENCHMARKCODE", "PERFORMANCEDATATYPE", "CURRENCYCODE",
        "PERFORMANCEFREQUENCY", "HISTORYDATE", "VALUE"
    ]]

def validate_benchmark_data(df: pd.DataFrame):
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

    # Remove duplicates inside the DataFrame
    df.drop_duplicates(subset=["BENCHMARKCODE", "HISTORYDATE"], inplace=True)

    return issues, df

def filter_existing_rows(conn, df):
    """
    Check which (BENCHMARKCODE, HISTORYDATE) combinations already exist in Snowflake
    and remove them from df before inserting.
    """
    benchmark_codes = "', '".join(df["BENCHMARKCODE"].unique())
    query = f"""
        SELECT BENCHMARKCODE, HISTORYDATE
        FROM {TABLE_NAME}
        WHERE BENCHMARKCODE IN ('{benchmark_codes}')
          AND HISTORYDATE BETWEEN '2024-12-01' AND '2024-12-31'
    """
    existing = pd.read_sql(query, conn)
    if existing.empty:
        return df

    merged = df.merge(existing, on=["BENCHMARKCODE", "HISTORYDATE"], how="left", indicator=True)
    new_df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    return new_df

def insert_benchmark_performance(conn, df: pd.DataFrame):
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
    polygon_benchmarks = ["SPY", "QQQ", "DIA", "IWM", "VTI"]
    all_benchmarks = []

    for ticker in polygon_benchmarks:
        print(f"Fetching {ticker} for Dec 2024 from Polygon.io...")
        df = fetch_benchmark_full_history(ticker, "2024-12-01", "2024-12-31")
        issues, validated_df = validate_benchmark_data(df)
        if issues:
            print(f"Issues for {ticker}:")
            for issue in issues:
                print(f" - {issue}")
        else:
            all_benchmarks.append(validated_df)

    if all_benchmarks:
        final_df = pd.concat(all_benchmarks, ignore_index=True)
        conn = get_snowflake_connection()

        # Filter out existing rows before insert
        final_df = filter_existing_rows(conn, final_df)

        if not final_df.empty:
            insert_benchmark_performance(conn, final_df)
        else:
            print("No new rows to insert — all data already exists.")

        conn.close()
