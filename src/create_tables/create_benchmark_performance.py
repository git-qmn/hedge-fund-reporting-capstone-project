import os
import logging
import dotenv
from src.db_connection import get_snowflake_connection
from dotenv import load_dotenv
load_dotenv()

TABLE_NAME = "BENCHMARKPERFORMANCE"

def create_benchmark_table(conn):
    """
    Create the BENCHMARKPERFORMANCE table in Snowflake if it does not exist.
    
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        BENCHMARKCODE VARCHAR PRIMARY KEY,
        CURRENCY VARCHAR,
        CURRENCYCODE VARCHAR,
        HISTORYDATE DATE,
        PERFORMANCEDATATYPE VARCHAR,
        PERFORMANCEFREQUENCY VARCHAR,
        VALUE FLOAT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created or verified: {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_benchmark_table(conn)
    conn.close()