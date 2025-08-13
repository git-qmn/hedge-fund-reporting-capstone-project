# /app/insert_disclosure_information.py

import uuid
import random
import pandas as pd
from datetime import datetime, timedelta
from src.db_connection import get_snowflake_connection
from src.open_ai_interactions import get_openai_client_obj, interact_with_chat_application

TABLE_NAME = "DISCLOSUREINFORMATION"

DISCLOSURE_TYPES = [
    "Regulatory",
    "Risk Notice",
    "Performance Notice",
    "Custom Footnote"
]

SOURCES = [
    "SEC",
    "Internal",
    "Generated",
    "Other"
]

def generate_disclosure_text(open_ai_client, disclosure_type):
    """
    Generate disclosure text using GPT for the given disclosure type.
    """
    prompt = (
        f"Write a professional {disclosure_type.lower()} disclosure statement "
        f"for an institutional investment fact sheet. Keep it concise, clear, "
        f"and compliant with financial industry standards."
    )
    response = interact_with_chat_application(prompt, open_ai_client)
    return response.choices[0].message.content.strip()

def generate_disclosure_data(open_ai_client, num_records=10):
    """
    Generate a DataFrame of synthetic disclosure records.
    """
    records = []
    today = datetime.today()

    for _ in range(num_records):
        disclosure_type = random.choice(DISCLOSURE_TYPES)
        disclosure_text = generate_disclosure_text(open_ai_client, disclosure_type)

        effective_date = today - timedelta(days=random.randint(0, 365 * 5))
        expiry_date = None if random.random() < 0.5 else effective_date + timedelta(days=random.randint(90, 730))

        records.append({
            "DISCLOSUREID": str(uuid.uuid4()),
            "DISCLOSURETYPE": disclosure_type,
            "DISCLOSURETEXT": disclosure_text,
            "EFFECTIVEDATE": effective_date.date(),
            "EXPIRYDATE": expiry_date.date() if expiry_date else None,
            "SOURCE": random.choice(SOURCES)
        })

    return pd.DataFrame(records)

def insert_disclosures(conn, df):
    """
    Insert disclosure records into DISCLOSUREINFORMATION table.
    """
    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        DISCLOSUREID, DISCLOSURETYPE, DISCLOSURETEXT,
        EFFECTIVEDATE, EXPIRYDATE, SOURCE
    ) VALUES (%s, %s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_sql, (
                row["DISCLOSUREID"],
                row["DISCLOSURETYPE"],
                row["DISCLOSURETEXT"],
                row["EFFECTIVEDATE"],
                row["EXPIRYDATE"],
                row["SOURCE"]
            ))
    conn.commit()
    print(f"Inserted {len(df)} disclosure records into {TABLE_NAME}.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    open_ai_client = get_openai_client_obj()

    df_disclosures = generate_disclosure_data(open_ai_client, num_records=20)
    print(df_disclosures.head())

    insert_disclosures(conn, df_disclosures)
    conn.close()