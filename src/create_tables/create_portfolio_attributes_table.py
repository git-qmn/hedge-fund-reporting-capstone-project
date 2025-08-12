
import os
import logging
import dotenv
from db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "PORTFOLIOATTRIBUTES"

def create_portfolio_attributes_table_if_not_exists(conn):
    """
    Create the PORTFOLIOATTRIBUTES table in Snowflake if it does not exist.
    
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        PORTFOLIOCODE STRING,
        ATTRIBUTETYPE STRING,
        ATTRIBUTETYPECODE STRING,
        ATTRIBUTETYPEVALUE STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    print(f"Ensured {TABLE_NAME} exists.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_portfolio_attributes_table_if_not_exists(conn)
    conn.close()