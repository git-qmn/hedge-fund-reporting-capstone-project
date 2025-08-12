# /app/create_benchmark_general_info.py
import os
import logging
import dotenv
from db_connection import get_snowflake_connection
from dotenv import load_dotenv
load_dotenv()

TABLE_NAME = "BENCHMARKGENERALINFO"

def create_benchmark_table(conn):
    """
    Create the BENCHMARKGENERALINFO table in Snowflake if it does not exist.
    
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        BENCHMARKCODE STRING PRIMARY KEY,
        BENCHMARKNAME STRING,
        BENCHMARKTYPE STRING,
        PROVIDER STRING,
        REGION STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created or verified: {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_benchmark_table(conn)
    conn.close()

