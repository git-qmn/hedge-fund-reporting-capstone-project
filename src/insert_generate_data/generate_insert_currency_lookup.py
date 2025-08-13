import os
import json
import logging
import dotenv
from src.db_connection import get_snowflake_connection
from dotenv import load_dotenv
load_dotenv()

def load_currency_data_from_json(json_path):
    """
    Load currency data from a JSON file.
    """
    with open(json_path, 'r') as file:
        return json.load(file)

def insert_currency_data(conn, currency_data):
    """
    Insert currency records into CURRENCYLOOKUP.
    """
    insert_sql = """
    INSERT INTO CURRENCYLOOKUP (CURRENCYCODE, CURRENCYNAME, PRICEMIN, PRICEMAX, COSTBASISMAX)
    VALUES (%s, %s, %s, %s, %s)
    """

    rows = []
    for code, values in currency_data["valid_currencies"].items():
        rows.append((
            code,
            values.get("name"),
            values.get("price_min"),
            values.get("price_max"),
            values.get("costbasis_max")
        ))

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    print(f"Inserted {len(rows)} rows into CURRENCYLOOKUP.")

if __name__ == "__main__":
    json_path = "src/valid_currencies.json"
    currency_data = load_currency_data_from_json(json_path)

    conn = get_snowflake_connection()
    insert_currency_data(conn, currency_data) 
    conn.close()