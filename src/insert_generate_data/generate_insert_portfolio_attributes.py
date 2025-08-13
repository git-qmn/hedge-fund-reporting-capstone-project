# /app/insert_portfolio_attributes.py
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from src.db_connection import get_snowflake_connection
from src.open_ai_interactions import get_openai_client_obj, interact_with_chat_application

TABLE_NAME = "PORTFOLIOATTRIBUTES"

ATTRIBUTE_OPTIONS = {
    "Strategy": [
        ("DLCV", "Domestic Large Cap Value"),
        ("GRO", "Growth-Oriented"),
        ("TH", "Thematic"),
        ("INX", "Index Replication")
    ],
    "AssetClass": [
        ("EQTY", "Equities"),
        ("FI", "Fixed Income"),
        ("MULTI", "Multi-Asset"),
        ("ALT", "Alternatives")
    ],
    "Vehicle": [
        ("MF", "Mutual Fund"),
        ("SA", "Separate Account"),
        ("SMA", "Sub-Advisory/Mutual Fund")
    ],
    "Vehicle Category": [
        ("POOL", "Pooled"),
        ("SEGR", "Segregated"),
        ("TH", "Taft-Hartley"),
        ("E/F", "Endowment/Foundation"),
        ("OTH", "Other")
    ]
}

def fetch_all_portfolio_codes(conn):
    query = "SELECT DISTINCT PORTFOLIOCODE FROM PORTFOLIOGENERALINFO;"
    df = pd.read_sql(query, conn)
    return df['PORTFOLIOCODE'].dropna().unique().tolist()

def generate_attribute_rows(portfolio_codes):
    rows = []
    for code in portfolio_codes:
        for attr_type, options in ATTRIBUTE_OPTIONS.items():
            attr_code, attr_value = random.choice(options)
            rows.append({
                "PORTFOLIOCODE": code,
                "ATTRIBUTETYPE": attr_type,
                "ATTRIBUTETYPECODE": attr_code,
                "ATTRIBUTETYPEVALUE": attr_value
            })
    return pd.DataFrame(rows)

def insert_portfolio_attributes(conn, df):
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        PORTFOLIOCODE, ATTRIBUTETYPE, ATTRIBUTETYPECODE, ATTRIBUTETYPEVALUE
    )
    VALUES (%s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_sql, (
                row['PORTFOLIOCODE'],
                row['ATTRIBUTETYPE'],
                row['ATTRIBUTETYPECODE'],
                row['ATTRIBUTETYPEVALUE']
            ))
    conn.commit()
    print(f"Inserted {len(df)} rows into {TABLE_NAME}.")

if __name__ == "__main__":
    conn = get_snowflake_connection()

    portfolio_codes = fetch_all_portfolio_codes(conn)
    print(f"Fetched {len(portfolio_codes)} portfolio codes.")

    df_attributes = generate_attribute_rows(portfolio_codes)
    print(df_attributes.head())

    insert_portfolio_attributes(conn, df_attributes)
    conn.close()
