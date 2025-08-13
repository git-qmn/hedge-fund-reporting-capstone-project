import logging
import dotenv
import os
dotenv.load_dotenv("local_config.env")
import snowflake.connector

def get_snowflake_connection():
    try:
        ctx = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        logging.info("Connected to Snowflake")
        return ctx
    except Exception as err:
        logging.error(f"Failed to connect to Snowflake. Error: {err}")
        return None