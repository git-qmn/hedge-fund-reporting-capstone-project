import os
import logging
from dotenv import load_dotenv
from src.db_connection import get_snowflake_connection

load_dotenv()

TABLE_NAME = "DISCLOSUREINFORMATION"

def create_disclosure_information_table(conn):
    """
    Create the DISCLOSUREINFORMATION table in Snowflake if it does not exist.
    This table stores qualitative disclosure and regulatory notes that appear
    in client materials and fact sheets.
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        DISCLOSUREID STRING PRIMARY KEY,
        DISCLOSURETYPE STRING,              -- e.g., Regulatory, Risk Notice, Performance Notice
        DISCLOSURETEXT STRING,               -- The actual disclosure content
        EFFECTIVEDATE DATE,                  -- Date from which the disclosure is valid
        EXPIRYDATE DATE,                      -- Optional date when disclosure is no longer valid
        SOURCE STRING,                        -- Origin of disclosure (e.g., "SEC", "Internal", "Generated")
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"Ensured {TABLE_NAME} exists.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    create_disclosure_information_table(conn)
    conn.close()

