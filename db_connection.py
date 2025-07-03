import pyodbc
import logging
import dotenv
import os
dotenv.load_dotenv("local_config.env")
import snowflake.connector

def azure_sql_connection(client):
    connection_string = os.getenv("DB_CONNECTION_STRING_TEMPLATE").replace("{{client_code}}", client)
    print(connection_string)
    cnxn = None
    try:
        # conn_engine = create_engine('mssql+pyodbc://'+username+':'+password+'@'+server+':1433'+'/'+database+'?driver=ODBC+Driver+17+for+SQL+Server')
        cnxn = pyodbc.connect(connection_string)
        logging.info(f"Connected to azure sql database. Client : {client}")
    except Exception as err:
        logging.error(f"Failed to connect to azure sql database. Error: {err}")
    return cnxn

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