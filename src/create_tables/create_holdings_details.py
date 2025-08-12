# app/insert_holdings.py

import os
import json
import logging
import dotenv
import numpy as np
import pandas as pd
from db_connection import get_snowflake_connection
from dotenv import load_dotenv
load_dotenv()

TABLE_NAME = "HOLDINGSDETAILS"

def create_holdings_details_table(conn):
    """
    Creates the HOLDINGSDETAILS table in Snowflake with the appropriate schema.
    
    """
    create_sql = f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} (
        CUSIP STRING,
        ISINCODE STRING,
        ISSUENAME STRING,
        TICKER STRING,
        PRICE FLOAT,
        SHARES FLOAT,
        MARKETVALUE FLOAT,
        CURRENCYCODE STRING,
        HQCOUNTRY STRING,
        ISSUECOUNTRY STRING,
        ASSETCLASSNAME STRING,
        BOOKVALUE FLOAT,
        COSTBASIS FLOAT,
        DIVIDENDYIELD FLOAT,
        HISTORYDATE TIMESTAMP_NTZ,
        POSITION_FLAG STRING,
        PRIMARYINDUSTRYNAME STRING,
        PRIMARYSECTORNAME STRING,
        PRIMARYSUBSECTORNAME STRING,
        REGIONNAME STRING,
        PORTFOLIOCODE STRING
    );
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
        print(f"Created table: {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_holdings_details_table(conn)
    conn.close()
