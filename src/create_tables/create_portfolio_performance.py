import os
import logging
import dotenv
from src.db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "PORTFOLIOPERFORMANCE"

def create_portfolio_performance_table(conn):
    """
    Create the essential PORTFOLIOPERFORMANCE table with only required fields.
    
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        PORTFOLIOCODE STRING,
        HISTORYDATE DATE,
        CURRENCYCODE STRING,
        PERFORMANCECATEGORYNAME STRING,
        PERFORMANCEINCEPTIONDATE DATE,
        PERFORMANCEFREQUENCY STRING,   
        PERFORMANCEFACTOR FLOAT,
        PERFORMANCETYPE STRING       
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created or verified: {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_portfolio_performance_table(conn)
    conn.close()
