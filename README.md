# Automated Daily Procurement Cost ETL Pipeline 🚀

## Executive Summary
This project is an end-to-end, fully automated Python ETL pipeline designed to solve a highly common business pain point: **manual daily financial reporting**. 

It automatically extracts nested API data from the Bank of Canada and Yahoo Finance, resolves timezone and holiday data gaps using Pandas forward-fill (`ffill`), and performs daily incremental upserts into a localized SQLite database.

## Business Value Delivered
* **100% Automation:** Replaced manual daily Excel data entry, saving approximately 10 hours of manual reporting work per month.
* **Data Reliability:** Implemented strict Data Quality (DQ) checks to prevent Null values and duplicate entries before loading into the production database.
* **Fault Tolerance:** Engineered a 10-day sliding window extraction with Idempotent Upsert (`INSERT ... ON CONFLICT DO UPDATE`) logic, ensuring the pipeline seamlessly handles long weekends, statutory holidays, and API downtimes without crashing or duplicating data.

## Tech Stack
* **Language:** Python 3
* **Extract (APIs):** `requests`, `yfinance`
* **Transform:** `pandas` (Time-series alignment, Forward-fill, Vectorized calculations)
* **Load & DB:** `sqlite3` (Relational schema design, Upsert logic)
* **Orchestration:** Windows Task Scheduler (`.bat` script deployment)
