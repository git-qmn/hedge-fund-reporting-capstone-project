# Assette MSBA Capstone – Snowflake Data Pipeline

## Overview

This repository contains a Python-based data ingestion and generation framework for simulating a large, diversified asset manager's datasets. It supports **multi-source data ingestion**, **data quality validation**, and **Snowflake integration**, in alignment with the Assette MSBA Capstone project requirements.

## Key Features

* Synthetic and API-sourced financial datasets (Yahoo Finance, Alpha Vantage, Polygon.io)
* Portfolio, benchmark, holdings, currency, and disclosure data
* Daily, monthly, and periodic performance updates
* Automated validation before insertion into Snowflake
* Duplicate prevention for existing records
* Configurable via `.env` file

---

## Quick Start

### 1. Clone the repository

```bash
git clone <repo-url>
cd <repo-directory>
```

### 2. Create and activate a virtual environment

**Windows**

```bash
python -m venv venv
venv\Scripts\activate
```

**Mac/Linux**

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create your `.env` file (copy from `.env.example`)

```bash
cp .env.example .env
```

Edit `.env` and add your Snowflake credentials + API keys:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ASSETTE_DB
SNOWFLAKE_SCHEMA=PUBLIC

ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
POLYGON_API_KEY=your_polygon_key
OPENAI_API_KEY=your_openai_key
```

### 5. Create all required Snowflake tables

Run each `create_*.py` file:

```bash
python create_benchmark_general_info.py
python create_benchmark_performance.py
python create_currency_lookup.py
python create_disclosure_info.py
python create_holdings_details.py
python create_portfolio_attributes_table.py
python create_portfolio_benchmark_association.py
python create_portfolio_general_info.py
python create_portfolio_performance.py
python create_product_master.py
python create_qualitative_info.py
```

### 6. Insert & generate data

Example:

```bash
python insert_generate_data/generate_insert_benchmark_general_info.py
python insert_generate_data/pull_insert_foreign_benchmark_performance.py
python insert_generate_data/pull_insert_polygon_benchmark.py
```

### 7. Verify in Snowflake

```sql
SELECT * FROM BENCHMARKPERFORMANCE LIMIT 10;
```

---

## Repository Structure

* **Table Creation Scripts** – create Snowflake tables
* **Data Insertion & Generation** (`/insert_generate_data`) – fetch, generate, validate, insert data
* **Config & Utilities** – `.env`, JSON config files, database connection helpers

---

## Validation & Quality Control

* Checks for missing or invalid data
* Removes duplicates
* Converts dates to Snowflake-compatible formats
* Skips inserting rows that already exist

---

## Data Sources

* Yahoo Finance
* Alpha Vantage
* Polygon.io
* OpenAI API

---

## License

This project is for educational purposes under the BU Questrom MSBA program.
