# Assette MSBA Capstone – Snowflake Data Pipeline

## Overview

This project is part of the BU Questrom MSBA Capstone Project 2025 in collaboration with Assette LLC, a SaaS provider to the institutional investment management industry. The goal is to create an automated pipeline that simulates a large, diversified asset manager's operations, from data ingestion to storage and validation in Snowflake.

The project implements an end-to-end data pipeline to populate a Snowflake database with synthetic and freely available data across multiple domains: performance, holdings, benchmarks, currencies, attributes, and qualitative disclosures. Data is ingested from APIs (Yahoo Finance, Alpha Vantage, Polygon.io), generated synthetically, validated for quality, and inserted into Snowflake using structured, modular Python scripts.

This system enables realistic simulations for sales demos, QA testing, and R&D without using proprietary client data.

---

## Key Features

* Multi-source data ingestion: Alpha Vantage, Polygon.io, Yahoo Finance, OpenAI
* Synthetic data generation for performance, holdings, and qualitative content
* Automated data validation and duplicate prevention
* Configurable environment variables for Snowflake and API keys
* Modular table creation and insertion scripts

---

## Contribution

Quan Nguyen
Victoria Carlsten

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

Add Snowflake credentials and API keys to `.env`.

### 5. Create required Snowflake tables

Run each `create_*.py` file to create schema.

### 6. Insert and generate data

Example:

```bash
python insert_generate_data/generate_insert_benchmark_general_info.py
python insert_generate_data/pull_insert_foreign_benchmark_performance.py
```

### 7. Verify in Snowflake

```sql
SELECT * FROM BENCHMARKPERFORMANCE LIMIT 10;
```

---

## Validation & Quality Control

* Checks for missing or invalid data
* Removes duplicates
* Converts dates to Snowflake-compatible formats
* Skips inserting rows that already exist

---

## Contact

For questions or suggestions, contact:

* Quan Minh Nguyen – [qmn@bu.edu](mailto:qmn@bu.edu)
* Victoria Carlsten – [carlsten@bu.edu](mailto:carlsten@bu.edu)

---

## Data Sources

* Yahoo Finance
* Alpha Vantage
* Polygon.io
* OpenAI API

---

## License

This project is for educational purposes under the BU Questrom MSBA program.
