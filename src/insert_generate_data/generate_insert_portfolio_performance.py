# /app/insert_portfolio_performance.py

import json
import random
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.db_connection import get_snowflake_connection
from src.open_ai_interactions import get_openai_client_obj, interact_with_chat_application

TABLE_NAME = "PORTFOLIOPERFORMANCE"

def get_portfolio_codes(conn):
    query = "SELECT DISTINCT PORTFOLIOCODE FROM PORTFOLIOGENERALINFO;"
    return pd.read_sql(query, conn)['PORTFOLIOCODE'].dropna().unique().tolist()

def generate_monthly_dates(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += relativedelta(months=1)

def generate_performance_data(portfolios, start_date, end_date):
    rows = []
    dates = list(generate_monthly_dates(start_date, end_date))

    for code in portfolios:
        inception_date = random.choice(dates)
        for date in dates:
            if date < inception_date:
                continue
            rows.append({
                "PORTFOLIOCODE": code,
                "HISTORYDATE": date.strftime('%Y-%m-%d'),
                "CURRENCYCODE": "USD",
                "PERFORMANCECATEGORYNAME": random.choice(["Equities", "Cash and Equiv."]),
                "PERFORMANCEINCEPTIONDATE": inception_date.strftime('%Y-%m-%d'),
                "PERFORMANCEFREQUENCY": "Monthly",
                "PERFORMANCEFACTOR": round(random.uniform(-0.05, 0.05), 6),
                "PERFORMANCETYPE": "Net Return"
            })
    return pd.DataFrame(rows)

def insert_performance_data(conn, df):
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        PORTFOLIOCODE, HISTORYDATE, CURRENCYCODE, PERFORMANCECATEGORYNAME,
        PERFORMANCEINCEPTIONDATE, PERFORMANCEFREQUENCY,
        PERFORMANCEFACTOR, PERFORMANCETYPE
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(sql, (
                row['PORTFOLIOCODE'],
                row['HISTORYDATE'],
                row['CURRENCYCODE'],
                row['PERFORMANCECATEGORYNAME'],
                row['PERFORMANCEINCEPTIONDATE'],
                row['PERFORMANCEFREQUENCY'],
                row['PERFORMANCEFACTOR'],
                row['PERFORMANCETYPE']
            ))
    conn.commit()
    print(f"Inserted {len(df)} rows into {TABLE_NAME}.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    portfolios = get_portfolio_codes(conn)

    start = datetime(2010, 1, 1)
    end = datetime(2025, 7, 31)

    df = generate_performance_data(portfolios, start, end)
    print(df.head())

    insert_performance_data(conn, df)
    conn.close()
