# src/create_tables/create_firm_and_strategy_info.py
import os
from dotenv import load_dotenv
from src.db_connection import get_snowflake_connection

load_dotenv()

FIRM_TABLE = "FIRMINFO"
STRATEGY_TABLE = "STRATEGYINFO"

def create_firm_info_table(conn):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {FIRM_TABLE} (
        SECTION STRING PRIMARY KEY,
        CONTENT STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created/verified: {FIRM_TABLE}")

def create_strategy_info_table(conn):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {STRATEGY_TABLE} (
        STRATEGYCODE STRING,
        SECTION      STRING,
        CONTENT      STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created/verified: {STRATEGY_TABLE}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_firm_info_table(conn)
    create_strategy_info_table(conn)
    conn.close()