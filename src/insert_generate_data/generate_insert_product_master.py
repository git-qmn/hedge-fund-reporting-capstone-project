import json
import random
import pandas as pd
from src.db_connection import get_snowflake_connection
from src.open_ai_interactions import interact_with_chat_application, get_openai_client_obj

TABLE_NAME = "PRODUCTMASTER"

def fetch_existing_product_codes(conn):
    """
    Fetch all existing PRODUCTCODEs from Snowflake to avoid duplicates.
    """
    query = f"SELECT PRODUCTCODE FROM {TABLE_NAME};"
    df = pd.read_sql(query, conn)
    return set(df['PRODUCTCODE'].tolist()) if not df.empty else set()


def fetch_strategies_from_snowflake(conn):
    """
    Fetch unique strategy sections from StrategyInfo table.
    """
    query = "SELECT DISTINCT SECTION FROM StrategyInfo;"
    df = pd.read_sql(query, conn)
    strategies = df['SECTION'].tolist()
    if not strategies:
        strategies = [
            "Long/Short Equity",
            "Global Macro",
            "Market Neutral",
            "ESG Focus",
            "Credit Arbitrage",
            "Event Driven",
            "Quantitative Equity",
            "Sector Rotation",
            "Multi-Asset",
            "Opportunistic Alpha"
        ]
    return strategies


def extract_json_from_response(content):
    """
    Extract JSON content from AI response.
    """
    content = content.strip()
    if content.startswith("```"):
        content = content.split("```")[1]
    return content.strip()


def generate_single_product(open_ai_client, product_index, strategy):
    """
    Generate one unique Novalon product using GPT, injecting a specific strategy.
    """
    prompt = (
        f"Generate one synthetic hedge fund product as a JSON object with these keys: "
        f"PRODUCTCODE (format NOVXXX where XXX is a 3-digit unique number), "
        f"PRODUCTNAME (must start with 'Novalon'), "
        f"ASSETCLASS (Equity, Fixed Income, Multi-Asset), "
        f"VEHICLETYPE (Separate Account, Pooled Vehicle, Mutual Fund), "
        f"VEHICLECATEGORY (Hedge Fund, ETF), "
        f"INCEPTIONDATE (YYYY-MM-DD between 2013-2023), "
        f"STATUS (Active or Closed), "
        f"CURRENCY (USD), "
        f"MANAGER (realistic name). "
        f"Use the strategy '{strategy}' for the STRATEGY field. "
        f"Return only valid JSON, no explanations."
    )

    response = interact_with_chat_application(prompt, open_ai_client)
    content = extract_json_from_response(response.choices[0].message.content.strip())
    print(f"DEBUG Product {product_index}:\n{content}")

    try:
        product = json.loads(content)
        product['STRATEGY'] = strategy  # enforce strategy
        return product
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse product {product_index}: {content}") from e


def generate_product_data(open_ai_client, conn, strategies, num_products=10):
    """
    Generate multiple products row by row with diverse strategies, avoiding duplicates.
    """
    existing_codes = fetch_existing_product_codes(conn)
    products = []

    for i in range(num_products):
        strategy = strategies[i % len(strategies)]
        try:
            product = generate_single_product(open_ai_client, i + 1, strategy)

            # Ensure PRODUCTCODE uniqueness
            while product['PRODUCTCODE'] in existing_codes:
                product['PRODUCTCODE'] = f"NOV{random.randint(100, 999)}"
            existing_codes.add(product['PRODUCTCODE'])

            products.append(product)
        except Exception as e:
            print(f"Error generating product {i+1}: {e}")

    if not products:
        raise ValueError("No valid products generated.")
    return pd.DataFrame(products)


def insert_into_product_master(conn, df):
    """
    Insert generated products into PRODUCTMASTER, skipping duplicates.
    """
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        PRODUCTCODE, PRODUCTNAME, STRATEGY, ASSETCLASS, VEHICLETYPE, VEHICLECATEGORY,
        INCEPTIONDATE, STATUS, CURRENCY, MANAGER
    )
    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM {TABLE_NAME} WHERE PRODUCTCODE = %s
    );
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_sql, (
                row.get('PRODUCTCODE'),
                row.get('PRODUCTNAME'),
                row.get('STRATEGY'),
                row.get('ASSETCLASS'),
                row.get('VEHICLETYPE'),
                row.get('VEHICLECATEGORY'),
                row.get('INCEPTIONDATE'),
                row.get('STATUS'),
                row.get('CURRENCY'),
                row.get('MANAGER'),
                row.get('PRODUCTCODE')  # for the WHERE NOT EXISTS
            ))
    conn.commit()
    print(f"Inserted {len(df)} new rows into {TABLE_NAME}.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    open_ai_client = get_openai_client_obj()

    # Fetch diverse strategies
    strategies = fetch_strategies_from_snowflake(conn)
    print(f"Using strategies: {strategies}")

    # Generate products
    product_df = generate_product_data(open_ai_client, conn, strategies, num_products=15)
    print("Generated Products:\n", product_df)

    # Insert into PRODUCTMASTER
    insert_into_product_master(conn, product_df)
