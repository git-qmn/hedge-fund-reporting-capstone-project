# /app/create_portfolio_benchmark_association.py
import os
import logging
import dotenv
from db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "PORTFOLIOBENCHMARKASSOCIATION"

def create_portfolio_benchmark_table(conn):
    """
    Create the PORTFOLIOBENCHMARKASSOCIATION table in Snowflake if it does not exist.
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        PORTFOLIOCODE STRING,
        BENCHMARKCODE STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created or verified: {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_portfolio_benchmark_table(conn)
    conn.close()
