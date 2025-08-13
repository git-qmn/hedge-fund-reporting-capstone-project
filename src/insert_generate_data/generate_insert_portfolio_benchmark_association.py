import yfinance as yf
import pandas as pd
import os
import logging
import dotenv
import random
from src.db_connection import get_snowflake_connection
from dotenv import load_dotenv
load_dotenv()

TABLE_NAME = "PORTFOLIOBENCHMARKASSOCIATION"

def fetch_portfolios(conn):
    query = "SELECT DISTINCT PORTFOLIOCODE FROM PORTFOLIOGENERALINFO;"
    df = pd.read_sql(query, conn)
    return df['PORTFOLIOCODE'].dropna().tolist()

def fetch_benchmarks(conn):
    query = "SELECT DISTINCT BENCHMARKCODE FROM BENCHMARKGENERALINFO;"
    df = pd.read_sql(query, conn)
    return df['BENCHMARKCODE'].dropna().tolist()

def generate_associations(portfolios, benchmarks):
    """
    Randomly assign 1 benchmark to each portfolio.
    """
    if not portfolios or not benchmarks:
        raise ValueError("Portfolios or Benchmarks list is empty.")

    records = []
    for code in portfolios:
        benchmark = random.choice(benchmarks)
        records.append((code, benchmark))

    return records

def insert_associations(conn, associations):
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        PORTFOLIOCODE, BENCHMARKCODE
    ) VALUES (%s, %s);
    """
    with conn.cursor() as cur:
        for portfolio_code, benchmark_code in associations:
            cur.execute(insert_sql, (portfolio_code, benchmark_code))
    conn.commit()
    print(f"Inserted {len(associations)} associations into {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    portfolios = fetch_portfolios(conn)
    benchmarks = fetch_benchmarks(conn)
    associations = generate_associations(portfolios, benchmarks)
    insert_associations(conn, associations)
    conn.close()
