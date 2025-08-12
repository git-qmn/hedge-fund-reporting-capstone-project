import os
import logging
import dotenv
from db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "CURRENCYLOOKUP"

def create_currency_lookup_table(conn):
    """
    Create the CURRENCYLOOKUP table with necessary fields.
    
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        CURRENCYCODE STRING PRIMARY KEY,
        CURRENCYNAME STRING,
        PRICEMIN FLOAT,
        PRICEMAX FLOAT,
        COSTBASISMAX FLOAT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Ensured {TABLE_NAME} exists.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_currency_lookup_table(conn)
    conn.close()
