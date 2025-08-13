import yfinance as yf
import pandas as pd
import random
import json
from datetime import datetime, timedelta
from src.db_connection import get_snowflake_connection
from dotenv import load_dotenv

load_dotenv()

def fetch_all_portfolio_codes(conn):
    """Fetch all portfolio codes from PORTFOLIOGENERALINFO table."""
    query = "SELECT DISTINCT PORTFOLIOCODE FROM PORTFOLIOGENERALINFO;"
    df = pd.read_sql(query, conn)
    return df['PORTFOLIOCODE'].dropna().unique().tolist()

def assign_portfolio_codes_to_tickers(tickers, portfolio_codes):
    """
    Randomly assign portfolio codes to tickers.
    
    Args:
        tickers (list): List of ticker symbols
        portfolio_codes (list): List of available portfolio codes
    
    Returns:
        dict: Dictionary mapping tickers to portfolio codes
    """
    if not portfolio_codes:
        raise ValueError("No portfolio codes available for assignment")
    
    ticker_portfolio_map = {}
    
    # Randomly assign portfolio codes to tickers
    for ticker in tickers:
        ticker_portfolio_map[ticker] = random.choice(portfolio_codes)
    
    return ticker_portfolio_map

def validate_and_impute_holdings_data(
    df,
    country_region_json='app/country_region_map.json',
    currencies_json='app/valid_currencies.json'
):
    """
    Validate and clean a holdings dataset with currency rules

    Returns:
        issues (list): Descriptions of all validation issues found.
        df (DataFrame): Cleaned and annotated DataFrame.
    """
    issues = []  # Collect all validation messages

    # === 1) Define expected columns (now includes PORTFOLIOCODE) ===
    required_columns = [
        "CUSIP", "ISINCODE", "ISSUENAME", "TICKER", "PRICE",
        "SHARES", "MARKETVALUE", "CURRENCYCODE", "HQCOUNTRY",
        "ISSUECOUNTRY", "REGIONNAME", "PRIMARYSECTORNAME",
        "PRIMARYSUBSECTORNAME", "PRIMARYINDUSTRYNAME",
        "DIVIDENDYIELD", "ASSETCLASSNAME", "BOOKVALUE",
        "COSTBASIS", "HISTORYDATE", "POSITION_FLAG", "PORTFOLIOCODE"
    ]

    # === 1A) COLUMN MATCHING ===
    actual_columns = set(df.columns)
    expected_columns = set(required_columns)

    missing_columns = expected_columns - actual_columns
    unexpected_columns = actual_columns - expected_columns

    if missing_columns:
        issues.append(f"Missing columns: {missing_columns}")

    if unexpected_columns:
        issues.append(f"Unexpected columns: {unexpected_columns}")

    if missing_columns:
        return issues, df

    # === 2) Convert HISTORYDATE ===
    df['HISTORYDATE'] = pd.to_datetime(df['HISTORYDATE'], errors='coerce')

    # === 3) Sort for time-series ops ===
    df = df.sort_values(by=['TICKER', 'HISTORYDATE'])

    # === 4) Impute PRICE within TICKER ===
    df['PRICE'] = df.groupby('TICKER')['PRICE'].ffill().bfill()

    # === 5) Load currency limits (with fallback) ===
    try:
        with open(currencies_json, 'r') as f:
            currency_data = json.load(f)['valid_currencies']
    except Exception as e:
        print(f"Warning: Could not load currencies JSON ({e}), using defaults")
        currency_data = {
            'USD': {'price_min': 0.01, 'price_max': 10000, 'costbasis_max': 1000000},
            'EUR': {'price_min': 0.01, 'price_max': 10000, 'costbasis_max': 1000000},
            'GBP': {'price_min': 0.01, 'price_max': 10000, 'costbasis_max': 1000000}
        }

    # === 5A) Validate PRICE and COSTBASIS per row ===
    for idx, row in df.iterrows():
        currency = row['CURRENCYCODE']
        price = row['PRICE']
        costbasis = row['COSTBASIS']

        # Use fallback if currency not found
        if currency in currency_data:
            pmin = currency_data[currency]['price_min']
            pmax = currency_data[currency]['price_max']
            cbmax = currency_data[currency]['costbasis_max']
        else:
            pmin, pmax, cbmax = 0.01, 10000, 1000000

        if not (pmin <= price <= pmax):
            issues.append(f"Row {idx}: PRICE {price} out of range for {currency} [{pmin}-{pmax}]")

        if costbasis < 0:
            issues.append(f"Row {idx}: COSTBASIS {costbasis} is negative")
        elif costbasis > cbmax:
            issues.append(f"Row {idx}: COSTBASIS {costbasis} exceeds max for {currency} [{cbmax}]")

    # === 5B) SHARES must be positive ===
    if not (df['SHARES'] > 0).all():
        issues.append("SHARES has zero or negative values")

    # === 5C) MARKETVALUE must match PRICE * SHARES ===
    df['MV_DIFF'] = abs(df['MARKETVALUE'] - (df['PRICE'] * df['SHARES']))
    if not (df['MV_DIFF'] < 0.01).all():
        issues.append("MARKETVALUE does not match PRICE * SHARES within tolerance")

    # === 5D) DIVIDENDYIELD must be reasonable ===
    if 'DIVIDENDYIELD' in df.columns:
        if not ((df['DIVIDENDYIELD'] >= 0) & (df['DIVIDENDYIELD'] <= 500)).all():
            issues.append("DIVIDENDYIELD out of 0–500% range")

    # === 5E) BOOKVALUE must be non-negative ===
    if 'BOOKVALUE' in df.columns:
        if not (df['BOOKVALUE'] >= 0).all():
            issues.append("BOOKVALUE has negative values")

    # === 6) Validate HQCOUNTRY, ISSUECOUNTRY, REGIONNAME (with fallback) ===
    try:
        with open(country_region_json, 'r') as f:
            country_region_map = json.load(f)
    except Exception as e:
        print(f"Warning: Could not load country-region JSON ({e}), using defaults")
        country_region_map = {
            "United States": "North America", "Canada": "North America", "Mexico": "North America",
            "United Kingdom": "Europe", "Germany": "Europe", "France": "Europe", 
            "Japan": "Asia", "China": "Asia", "India": "Asia", "South Korea": "Asia",
            "Brazil": "South America", "Argentina": "South America",
            "Australia": "Oceania", "New Zealand": "Oceania"
        }

    valid_countries = set(country_region_map.keys())
    valid_regions = set(country_region_map.values())

    hq_countries = set(df['HQCOUNTRY'].dropna().unique())
    invalid_hq = hq_countries - valid_countries
    if invalid_hq:
        issues.append(f"Invalid HQCOUNTRY values: {invalid_hq}")

    issue_countries = set(df['ISSUECOUNTRY'].dropna().unique())
    invalid_issue = issue_countries - valid_countries
    if invalid_issue:
        issues.append(f"Invalid ISSUECOUNTRY values: {invalid_issue}")

    my_regions = set(df['REGIONNAME'].dropna().unique())
    invalid_regions = my_regions - valid_regions
    if invalid_regions:
        issues.append(f"Invalid REGIONNAME values: {invalid_regions}")

    def check_region_match(row):
        expected_region = country_region_map.get(row['HQCOUNTRY'])
        return row['REGIONNAME'] == expected_region

    df['RegionMatch'] = df.apply(check_region_match, axis=1)
    if not df['RegionMatch'].all():
        mismatches = df[df['RegionMatch'] == False][['HQCOUNTRY', 'REGIONNAME']].drop_duplicates()
        issues.append(f"HQCOUNTRY-region mismatches: {mismatches.to_dict(orient='records')}")

    # === 7) Validate CURRENCYCODE ===
    valid_currency_codes = set(currency_data.keys())
    if not df['CURRENCYCODE'].isin(valid_currency_codes).all():
        invalid_currencies = df[~df['CURRENCYCODE'].isin(valid_currency_codes)]['CURRENCYCODE'].unique()
        issues.append(f"Invalid CURRENCYCODE values: {invalid_currencies}")

    # === 8) Validate POSITION_FLAG ===
    if 'POSITION_FLAG' in df.columns:
        if not df['POSITION_FLAG'].isin(["LONG", "SHORT"]).all():
            invalid_flags = df[~df['POSITION_FLAG'].isin(["LONG", "SHORT"])]['POSITION_FLAG'].unique()
            issues.append(f"Invalid POSITION_FLAG values: {invalid_flags}")

    # === 9) Validate HISTORYDATE range ===
    min_date = pd.to_datetime("2000-01-01")
    max_date = pd.to_datetime("today") + pd.Timedelta(days=1)
    if not ((df['HISTORYDATE'] >= min_date) & (df['HISTORYDATE'] <= max_date)).all():
        issues.append("Some HISTORYDATE values are out of expected range")

    # === 10) Check for nulls in critical fields (now includes PORTFOLIOCODE) ===
    for col in ["CUSIP", "ISINCODE", "TICKER", "SHARES", "PORTFOLIOCODE"]:
        if df[col].isnull().any():
            issues.append(f"Null values found in {col}")

    # === 11) Check for duplicate TICKER + HISTORYDATE ===
    if df.duplicated(subset=['TICKER', 'HISTORYDATE']).any():
        issues.append("Duplicate TICKER + HISTORYDATE rows found")

    # === 12) Drop helpers ===
    df.drop(columns=['MV_DIFF', 'RegionMatch'], inplace=True, errors='ignore')

    return issues, df

def get_tickers():
    """Get list of tickers to process."""
    print("Loading S&P 500 tickers from Wikipedia...")
    try:
        sp500_tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]['Symbol'].tolist()
    except Exception as e:
        print(f"Warning: Could not load S&P 500 list ({e}), using fallback list")
        sp500_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']
    
    extra_tickers = ['SHOP', 'SE', 'BIDU', 'JD', 'MELI', 'TSM', 'TCEHY']
    tickers = list(set(sp500_tickers + extra_tickers))
    random.shuffle(tickers)
    return tickers[:500]  # Limit to 500 tickers

def generate_fake_cusip():
    return ''.join(random.choices('0123456789ABCDEFGHJKLMNPQRSTUVWXYZ', k=9))

def generate_fake_isin(ticker):
    return f"US{''.join(random.choices('0123456789', k=10))}"

def get_random_date():
    return (datetime.today() - timedelta(days=random.randint(0, 3*365))).strftime('%Y-%m-%d')

def derive_subsector(industry):
    keywords = {
        "Software": "Application Software",
        "Hardware": "Consumer Electronics", 
        "Semiconductors": "Chip Makers",
        "Bank": "Commercial Banks",
        "Retail": "E-commerce",
        "Media": "Streaming",
        "Pharma": "Biopharma"
    }
    for key, value in keywords.items():
        if key.lower() in industry.lower():
            return value
    return "Other Subsector"

def generate_holdings_data(tickers, ticker_portfolio_map, max_count=2000):
    """Generate holdings data using yfinance with portfolio code assignment."""
    print(f"Generating holdings data for up to {max_count} records...")
    
    region_map = {
        "United States": "North America", "Canada": "North America", "Mexico": "North America",
        "United Kingdom": "Europe", "Germany": "Europe", "France": "Europe", "Sweden": "Europe", "Netherlands": "Europe",
        "Japan": "Asia", "China": "Asia", "India": "Asia", "South Korea": "Asia", "Singapore": "Asia",
        "Brazil": "South America", "Argentina": "South America", "Chile": "South America",
        "South Africa": "Africa", "Nigeria": "Africa", "Egypt": "Africa",
        "Australia": "Oceania", "New Zealand": "Oceania"
    }
    
    data = []
    count = 0
    
    for ticker in tickers:
        if count >= max_count:
            break
            
        try:
            print(f"Processing {ticker} ({count + 1}/{max_count})")
            stock = yf.Ticker(ticker)
            info = stock.info
            
            price = info.get("currentPrice") or info.get("regularMarketPrice") or round(random.uniform(10, 500), 2)
            shares = random.randint(100, 10000)
            market_value = price * shares
            dividend_yield = info.get("dividendYield", 0) or 0
            sector = info.get("sector", "Unknown")
            industry = info.get("industry", "Unknown")
            country = info.get("country", "United States")
            
            row = {
                "CUSIP": generate_fake_cusip(),
                "ISINCODE": generate_fake_isin(ticker),
                "ISSUENAME": info.get("longName", ticker),
                "TICKER": ticker,
                "PRICE": round(price, 2),
                "SHARES": shares,
                "MARKETVALUE": round(market_value, 2),
                "CURRENCYCODE": info.get("currency", "USD"),
                "HQCOUNTRY": country,
                "ISSUECOUNTRY": country,
                "REGIONNAME": region_map.get(country, "North America"),
                "PRIMARYSECTORNAME": sector,
                "PRIMARYSUBSECTORNAME": derive_subsector(industry),
                "PRIMARYINDUSTRYNAME": industry,
                "DIVIDENDYIELD": round(dividend_yield * 100, 2) if dividend_yield else 0,
                "ASSETCLASSNAME": "Equity",
                "BOOKVALUE": round(random.uniform(0.5, 2.0) * shares, 2),
                "COSTBASIS": round(random.uniform(0.6, 1.1) * price, 2),
                "HISTORYDATE": get_random_date(),
                "POSITION_FLAG": "LONG" if random.random() > 0.1 else "SHORT",
                "PORTFOLIOCODE": ticker_portfolio_map.get(ticker, "DEFAULT_PORTFOLIO")
            }
            data.append(row)
            count += 1
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue
    
    print(f"Generated {len(data)} holdings records")
    return pd.DataFrame(data)

def upload_to_snowflake(df, table_name="HOLDINGSDETAILS"):
    """Upload DataFrame to Snowflake."""
    if df.empty:
        print("No data to upload.")
        return False
    
    print(f"Preparing to upload {len(df)} rows to {table_name}...")
    
    # Convert HISTORYDATE to date format
    df['HISTORYDATE'] = pd.to_datetime(df['HISTORYDATE']).dt.date
    
    # Get connection
    conn = get_snowflake_connection()
    if conn is None:
        print("Failed to connect to Snowflake")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            print(f"Table {table_name} does not exist. Please create it first.")
            return False
        
        print(f"Uploading data to {table_name}...")
        
        # Prepare INSERT statement
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.values]
        
        # Execute batch insert
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        
        print(f"Successfully uploaded {len(data_tuples)} rows to {table_name}")
        return True
        
    except Exception as e:
        print(f"Error uploading to Snowflake: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    """Main execution function."""
    try:
        # Step 1: Get database connection and fetch portfolio codes
        print("Connecting to Snowflake to fetch portfolio codes...")
        conn = get_snowflake_connection()
        if conn is None:
            print("Failed to connect to Snowflake. Cannot fetch portfolio codes.")
            return
        
        try:
            portfolio_codes = fetch_all_portfolio_codes(conn)
            print(f"Fetched {len(portfolio_codes)} portfolio codes: {portfolio_codes[:5]}{'...' if len(portfolio_codes) > 5 else ''}")
        except Exception as e:
            print(f"Error fetching portfolio codes: {e}")
            return
        finally:
            conn.close()
        
        if not portfolio_codes:
            print("No portfolio codes found in PORTFOLIOGENERALINFO table.")
            return
        
        # Step 2: Get tickers
        tickers = get_tickers()
        print(f"Using {len(tickers)} tickers")
        
        # Step 3: Create ticker to portfolio mapping
        print("Assigning portfolio codes to tickers...")
        ticker_portfolio_map = assign_portfolio_codes_to_tickers(tickers, portfolio_codes)
        
        # Show sample assignments
        sample_assignments = list(ticker_portfolio_map.items())[:5]
        print(f"Sample ticker-portfolio assignments: {sample_assignments}")
        
        # Step 4: Generate holdings data with portfolio assignments
        df_holdings = generate_holdings_data(tickers, ticker_portfolio_map, max_count=2000)
        
        if df_holdings.empty:
            print("No holdings data generated. Exiting.")
            return
        
        print(f"Generated holdings data shape: {df_holdings.shape}")
        print("Sample data:")
        print(df_holdings[['TICKER', 'PORTFOLIOCODE', 'ISSUENAME', 'PRICE', 'SHARES']].head())
        
        # Step 5: Validate data
        print("\nValidating holdings data...")
        issues, df_clean = validate_and_impute_holdings_data(df_holdings)
        
        if issues:
            print("Validation Issues Found:")
            for issue in issues[:10]:  # Show first 10 issues
                print(f"- {issue}")
            if len(issues) > 10:
                print(f"... and {len(issues) - 10} more issues")
        else:
            print("All validation checks passed!")
        
        print(f"Clean data shape: {df_clean.shape}")
        
        # Step 6: Upload to Snowflake
        if not df_clean.empty:
            success = upload_to_snowflake(df_clean)
            if success:
                print("Data successfully uploaded to HOLDINGSDETAILS table!")
                
                # Show portfolio distribution
                portfolio_dist = df_clean['PORTFOLIOCODE'].value_counts()
                print(f"\nPortfolio distribution:")
                print(portfolio_dist.head(10))
            else:
                print("Upload failed.")
        else:
            print("No clean data to upload.")
            
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()