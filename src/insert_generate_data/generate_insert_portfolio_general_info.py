# /app/insert_portfolio_general_info.py

import json
import random
import pandas as pd
from datetime import datetime, timedelta
from src.db_connection import get_snowflake_connection
from src.open_ai_interactions import get_openai_client_obj, interact_with_chat_application

TABLE_NAME = "PORTFOLIOGENERALINFO"
PRODUCT_TABLE = "PRODUCTMASTER"

def fetch_existing_portfolio_codes(conn):
    query = f"SELECT PORTFOLIOCODE FROM {TABLE_NAME};"
    df = pd.read_sql(query, conn)
    return set(df['PORTFOLIOCODE'].tolist()) if not df.empty else set()

def fetch_existing_product_codes(conn):
    query = f"SELECT DISTINCT PRODUCTCODE FROM {PRODUCT_TABLE};"
    df = pd.read_sql(query, conn)
    return df['PRODUCTCODE'].dropna().tolist()

def extract_json_from_response(content):
    content = content.strip()
    if content.startswith("```"):
        content = content.split("```")[1]
    return content.strip()

def generate_single_portfolio(open_ai_client, index, strategy):
    """
    Generate one portfolio object using GPT with creative naming.
    """
    prompt = (
        f"Generate ONE synthetic institutional portfolio as a valid JSON object. "
        f"Required fields: "
        f"PORTFOLIOCODE (format NVLN###), "
        f"NAME (be original: use diverse terms like Trust, Plan, Fund, Reserve, Strategy, Overlay, Allocation, Opportunities, Capital, etc.), "
        f"PORTFOLIOCATEGORY (randomly pick from: 'Individual Account', 'Composite', 'Consolidated'), "
        f"INVESTMENTSTYLE (randomly pick from: 'Growth', 'Value', 'Index', 'Thematic', 'Balanced', 'Opportunistic', 'ESG Focus', 'Dividend Yield', 'Capital Appreciation'). "
        f"Examples of good names: 'Starlight Opportunity Allocation', 'Silverwood Retirement Trust', "
        f"'Apex Climate Composite', 'Ironleaf Strategic Reserve', 'Blue Horizon Capital Pool'. "
        f"Respond with ONE valid JSON object only. No list. No extra text. No prefix/suffix. Just raw JSON."
    )



    response = interact_with_chat_application(prompt, open_ai_client)
    content = extract_json_from_response(response.choices[0].message.content.strip())

    try:
        portfolio = json.loads(content)
        portfolio["INVESTMENTSTYLE"] = portfolio.get("INVESTMENTSTYLE", random.choice(["Growth", "Value", "Index", "Balanced", "Thematic"]))
        return portfolio
    except Exception as e:
        raise ValueError(f"Error parsing GPT portfolio {index}: {content}") from e

def generate_random_date(start="2010-01-01", end="2023-12-31"):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    delta = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, delta))

def generate_portfolio_data(open_ai_client, conn, strategy, num_portfolios=10):
    existing_codes = fetch_existing_portfolio_codes(conn)
    product_codes = fetch_existing_product_codes(conn)
    portfolios = []

    for i in range(num_portfolios):
        try:
            portfolio = generate_single_portfolio(open_ai_client, i + 1, strategy)

            # Ensure unique PORTFOLIOCODE
            while portfolio['PORTFOLIOCODE'] in existing_codes:
                portfolio['PORTFOLIOCODE'] = f"NVLN{random.randint(100,999)}"
            existing_codes.add(portfolio['PORTFOLIOCODE'])

            open_date = generate_random_date()
            perf_date = open_date + timedelta(days=random.randint(30, 365))
            currency = random.choice([("USD", "US Dollar"), ("EUR", "Euro"), ("JPY", "Japanese Yen")])

            portfolio.update({
                "OPENDATE": open_date.date(),
                "PERFORMANCEINCEPTIONDATE": perf_date.date(),
                "ISBEGINOFDAYPERFORMANCE": random.choice([True, False]),
                "BASECURRENCYCODE": currency[0],
                "BASECURRENCYNAME": currency[1],
                "PRODUCTCODE": random.choice(product_codes)
            })

            portfolios.append(portfolio)

        except Exception as e:
            print(f"Failed to generate portfolio {i+1}: {e}")

    return pd.DataFrame(portfolios)

def insert_into_portfolio_table(conn, df):
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        PORTFOLIOCODE, NAME, INVESTMENTSTYLE, PORTFOLIOCATEGORY,
        OPENDATE, PERFORMANCEINCEPTIONDATE, ISBEGINOFDAYPERFORMANCE,
        BASECURRENCYCODE, BASECURRENCYNAME, PRODUCTCODE
    )
    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    WHERE NOT EXISTS (SELECT 1 FROM {TABLE_NAME} WHERE PORTFOLIOCODE = %s);
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_sql, (
                row.get('PORTFOLIOCODE'),
                row.get('NAME'),
                row.get('INVESTMENTSTYLE'),
                row.get('PORTFOLIOCATEGORY'),
                row.get('OPENDATE'),
                row.get('PERFORMANCEINCEPTIONDATE'),
                row.get('ISBEGINOFDAYPERFORMANCE'),
                row.get('BASECURRENCYCODE'),
                row.get('BASECURRENCYNAME'),
                row.get('PRODUCTCODE'),
                row.get('PORTFOLIOCODE')
            ))
    conn.commit()
    print(f"Inserted {len(df)} new rows into {TABLE_NAME}.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    open_ai_client = get_openai_client_obj()

    strategy = "Global Value Equity"
    df = generate_portfolio_data(open_ai_client, conn, strategy, num_portfolios=50)
    print(df)

    insert_into_portfolio_table(conn, df)
    conn.close()
    print("Done.")
