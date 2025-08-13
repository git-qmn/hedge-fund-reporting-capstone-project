# Yfinance Source

import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
from src.db_connection import get_snowflake_connection
import numpy as np

load_dotenv()

def get_existing_data_info(conn, table_name):
    """Get information about existing data in Snowflake table."""
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Check if table exists by trying to query it
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1")
            cursor.fetchone()
            print(f"Table {table_name} exists")
        except:
            print(f"Table {table_name} doesn't exist.")
            return set(), None
        
        # Get existing benchmark codes and their latest dates
        cursor.execute(f"""
            SELECT BENCHMARKCODE, MAX(HISTORYDATE) as MAX_DATE
            FROM {table_name}
            GROUP BY BENCHMARKCODE
        """)
        
        existing_data = cursor.fetchall()
        existing_benchmarks = {}
        for row in existing_data:
            existing_benchmarks[row[0]] = row[1]
        
        print(f"Found existing data for {len(existing_benchmarks)} benchmark codes")
        for code, max_date in existing_benchmarks.items():
            print(f"  {code}: latest date = {max_date}")
        
        return set(existing_benchmarks.keys()), existing_benchmarks
        
    except Exception as e:
        print(f"Error checking existing data: {e}")
        return set(), None
    finally:
        if cursor:
            cursor.close()

def fetch_benchmark_full_history(ticker, from_date, to_date, column_to_use="Close"):
    """
    Fetches daily historical price data for a single ticker using yfinance 
    and returns a standardized DataFrame.
    """
    try:
        data = yf.download(ticker, start=from_date, end=to_date)
        
        if data.empty:
            print(f"Warning: No data returned for {ticker}")
            return pd.DataFrame()
        
        # Handle MultiIndex columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        data = data.reset_index()
        
        # CRITICAL FIX: Filter out rows with NaN values in the price column FIRST
        print(f"Before filtering: {ticker} has {len(data)} total rows")
        print(f"NaN values in {column_to_use}: {data[column_to_use].isna().sum()}")
        
        # Remove rows where the price column has NaN values
        data_clean = data.dropna(subset=[column_to_use])
        print(f"After filtering: {ticker} has {len(data_clean)} valid rows")
        
        if data_clean.empty:
            print(f"Warning: No valid data for {ticker} after filtering NaN values")
            return pd.DataFrame()
        
        # Create the standardized DataFrame structure using clean data
        num_rows = len(data_clean)
        result_df = pd.DataFrame({
            "BENCHMARKCODE": [ticker.upper()] * num_rows,
            "PERFORMANCEDATATYPE": ["Prices"] * num_rows,
            "CURRENCYCODE": ["USD"] * num_rows,
            "CURRENCY": ["US Dollar"] * num_rows,
            "PERFORMANCEFREQUENCY": ["Daily"] * num_rows,
            "HISTORYDATE": pd.to_datetime(data_clean["Date"]),
            "VALUE": data_clean[column_to_use].astype(float)
        })
        
        # Verify no NaN values remain
        nan_check = result_df.isnull().sum().sum()
        print(f"Final NaN check for {ticker}: {nan_check} NaN values")
        
        return result_df
        
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()

def filter_new_data(df, existing_benchmarks):
    """Filter dataframe to only include new data that doesn't exist in Snowflake."""
    if not existing_benchmarks or df.empty:
        return df
    
    new_data_list = []
    
    for benchmark_code in df['BENCHMARKCODE'].unique():
        benchmark_data = df[df['BENCHMARKCODE'] == benchmark_code].copy()
        
        if benchmark_code in existing_benchmarks:
            # Only keep data after the latest existing date
            latest_existing_date = existing_benchmarks[benchmark_code]
            benchmark_data = benchmark_data[
                benchmark_data['HISTORYDATE'] > latest_existing_date
            ]
            
            if not benchmark_data.empty:
                print(f"Found {len(benchmark_data)} new records for {benchmark_code} after {latest_existing_date}")
            else:
                print(f"No new data for {benchmark_code} after {latest_existing_date}")
        else:
            print(f"New benchmark {benchmark_code}: {len(benchmark_data)} total records")
        
        if not benchmark_data.empty:
            new_data_list.append(benchmark_data)
    
    if new_data_list:
        return pd.concat(new_data_list, ignore_index=True)
    else:
        return pd.DataFrame()

def validate_data_structure(df, conn, table_name):
    """Validate that DataFrame structure matches existing Snowflake table."""
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Simple query to get table columns
        query = f"DESCRIBE TABLE {table_name}"
        cursor.execute(query)
        
        table_columns = cursor.fetchall()
        if not table_columns:
            print(f"Warning: Could not retrieve schema for table {table_name}")
            return False
        
        # Extract column names from DESCRIBE result (first column is the name)
        table_column_names = []
        for col in table_columns:
            table_column_names.append(col[0])
            
        df_column_names = list(df.columns)
        
        print(f"Table {table_name} columns: {sorted(table_column_names)}")
        print(f"DataFrame columns: {sorted(df_column_names)}")
        
        # Convert to sets for comparison
        table_cols_set = set(table_column_names)
        df_cols_set = set(df_column_names)
        
        # Check if DataFrame columns match table columns
        missing_in_df = table_cols_set - df_cols_set
        extra_in_df = df_cols_set - table_cols_set
        
        if missing_in_df or extra_in_df:
            print("VALIDATION FAILED - Column mismatch:")
            if missing_in_df:
                print(f"  Missing columns in DataFrame: {sorted(list(missing_in_df))}")
            if extra_in_df:
                print(f"  Extra columns in DataFrame: {sorted(list(extra_in_df))}")
            return False
        
        print("Data structure validation passed")
        return True
        
    except Exception as e:
        print(f"Error validating data structure: {e}")
        return False
    finally:
        if cursor:
            cursor.close()

def clean_data_for_snowflake(df):
    """Clean DataFrame to ensure compatibility with Snowflake."""
    if df.empty:
        return df
    
    print("Cleaning data for Snowflake compatibility...")
    
    # Check for any remaining NaN values
    nan_counts = df.isnull().sum()
    total_nans = nan_counts.sum()
    
    if total_nans > 0:
        print(f"Found {total_nans} NaN values:")
        for col, count in nan_counts.items():
            if count > 0:
                print(f"  {col}: {count} NaN values")
        
        # Drop rows with any NaN values
        original_length = len(df)
        df_clean = df.dropna()
        dropped_rows = original_length - len(df_clean)
        
        if dropped_rows > 0:
            print(f"Dropped {dropped_rows} rows containing NaN values")
        
        return df_clean
    else:
        print("No NaN values found - data is clean")
        return df

def upload_to_snowflake(df, table_name):
    """Upload DataFrame directly to Snowflake with duplicate prevention and validation."""
    if df.empty:
        print("No data to upload.")
        return
    
    print(f"Preparing to upload {df.shape[0]} rows and {df.shape[1]} columns.")
    
    # Clean data before upload
    df = clean_data_for_snowflake(df)
    
    if df.empty:
        print("No data remaining after cleaning.")
        return
    
    # Convert HISTORYDATE column to date
    if "HISTORYDATE" in df.columns:
        df["HISTORYDATE"] = pd.to_datetime(df["HISTORYDATE"]).dt.date
    
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    if conn is None:
        print("Failed to connect to Snowflake. Check your connection and credentials.")
        return
    
    try:
        # Validate data structure against existing table
        if not validate_data_structure(df, conn, table_name):
            print("Upload cancelled due to validation failure.")
            return
        
        # Check existing data
        existing_benchmark_codes, existing_benchmarks = get_existing_data_info(conn, table_name)
        
        # Filter to only new data
        filtered_df = filter_new_data(df, existing_benchmarks)
        
        if filtered_df.empty:
            print("No new data to upload after filtering for duplicates.")
            return
        
        print(f"Uploading {len(filtered_df)} new records to Snowflake table: {table_name}")
        
        # Final data validation before upload
        print("Final data validation:")
        print(f"  Data types: {dict(filtered_df.dtypes)}")
        print(f"  NaN check: {filtered_df.isnull().sum().sum()} total NaN values")
        print(f"  Sample VALUE column: {filtered_df['VALUE'].head().tolist()}")
        
        # Manual SQL insertion approach to avoid write_pandas ON_ERROR issue
        cursor = conn.cursor()
        
        # Prepare the INSERT statement
        columns = list(filtered_df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples for insertion
        data_tuples = [tuple(row) for row in filtered_df.values]
        
        # Execute batch insert
        cursor.executemany(insert_sql, data_tuples)
        
        # Commit the transaction
        conn.commit()
        
        print(f"Data uploaded successfully: {len(data_tuples)} rows to {table_name}")
        cursor.close()
            
    except Exception as e:
        print(f"Error during upload: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("Snowflake connection closed.")

def fetch_all_benchmark_data(benchmarks, from_date, to_date):
    """Fetch data for all benchmarks and return combined DataFrame."""
    all_benchmarks = []
    
    for ticker in benchmarks:
        print(f"Fetching {ticker} from Yahoo Finance...")
        df = fetch_benchmark_full_history(ticker, from_date, to_date)
        print(f"{ticker}: {df.shape[0]} rows")
        
        if not df.empty:
            print(df.head(2))
            print(df.tail(2))
            all_benchmarks.append(df)
    
    if all_benchmarks:
        combined_df = pd.concat(all_benchmarks, ignore_index=True)
        print(f"\nTotal combined data: {combined_df.shape[0]} rows")
        
        # Ensure clean column names (no tuples)
        if isinstance(combined_df.columns, pd.MultiIndex):
            combined_df.columns = combined_df.columns.get_level_values(0)
        
        # Force clean string column names
        clean_columns = []
        for col in combined_df.columns:
            if isinstance(col, tuple):
                clean_columns.append(col[0])
            else:
                clean_columns.append(str(col))
        combined_df.columns = clean_columns
        
        print(f"Final column names: {list(combined_df.columns)}")
        return combined_df
    else:
        print("No data fetched.")
        return pd.DataFrame()

def main():
    """Main function to orchestrate the benchmark data extraction and upload."""
    yfinance_benchmarks = [
        "SPY", "QQQ", "DIA", "IWM", "VTI", "EFA", "EEM", "AGG", 
        "LQD", "HYG", "GLD", "VNQ", "TLT", "IVV", "VWO", "BND", 
        "SHV", "SCHF", "IEFA", "XLF"
    ]
    
    from_date = "1999-01-01"
    to_date = datetime.today().strftime("%Y-%m-%d")
    table_name = "BENCHMARKPERFORMANCE"
    
    # Fetch all benchmark data
    combined_df = fetch_all_benchmark_data(yfinance_benchmarks, from_date, to_date)
    
    # Upload to Snowflake with validation and duplicate prevention
    if not combined_df.empty:
        upload_to_snowflake(combined_df, table_name)
    else:
        print("No data to upload.")

if __name__ == "__main__":
    main()