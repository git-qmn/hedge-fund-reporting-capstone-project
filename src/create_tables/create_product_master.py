import os
import logging
import dotenv
from db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "PRODUCTMASTER"

def create_product_master_table(conn):
    """
    Create the PRODUCTMASTER table in Snowflake if it does not exist.

    """
    create_sql = f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} (
        PRODUCTCODE STRING PRIMARY KEY,
        PRODUCTNAME STRING NOT NULL,
        STRATEGY STRING NOT NULL,
        ASSETCLASS STRING NOT NULL,
        VEHICLETYPE STRING NOT NULL,
        VEHICLECATEGORY STRING NOT NULL,
        INCEPTIONDATE DATE NOT NULL,
        STATUS STRING NOT NULL,
        CURRENCY STRING NOT NULL,
        MANAGER STRING
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Created or replaced: {TABLE_NAME}")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_product_master_table(conn)
    conn.close()