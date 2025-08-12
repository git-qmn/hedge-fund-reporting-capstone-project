import os
import logging
import dotenv
from db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()
TABLE_NAME = "PORTFOLIOGENERALINFO"

def create_portfolio_table_if_not_exists(conn):
    """
    Ensure the portfolio table exists in Snowflake.
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        PORTFOLIOCODE STRING PRIMARY KEY,
        NAME STRING,
        INVESTMENTSTYLE STRING,
        PORTFOLIOCATEGORY STRING,
        OPENDATE DATE,
        PERFORMANCEINCEPTIONDATE DATE,
        ISBEGINOFDAYPERFORMANCE BOOLEAN,
        BASECURRENCYCODE STRING,
        BASECURRENCYNAME STRING,
        PRODUCTCODE STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    print(f"Ensured {TABLE_NAME} exists.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_portfolio_table_if_not_exists(conn)
    conn.close()