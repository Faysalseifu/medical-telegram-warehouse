# Medical Telegram Warehouse
A data pipeline and analytics API for medical Telegram channels. The project scrapes channel messages, loads them into a warehouse, enriches image content with YOLO, and serves business-facing insights through a FastAPI service.

## Business Problem
Medical product activity on Telegram is fast-moving and fragmented. Sellers, analysts, and operators need a way to collect messages, track channel performance, inspect image-based product evidence, and turn raw Telegram activity into structured warehouse tables and reports.

## Solution Overview
This project combines four layers: Telegram scraping for raw collection, Postgres and dbt for warehouse modeling, YOLO-based image enrichment for product detection, and a FastAPI analytics layer for reporting, search, and channel insights. Dagster orchestrates the pipeline so raw data can flow from ingestion to transformed marts in a repeatable way.

## Key Results
   - Metric 1: XX% improvement in reporting speed
   - Metric 2: $XX saved in manual analysis effort
   - Metric 3: X hours reduced in weekly monitoring

## Quick Start
```bash
git clone https://github.com/username/project
cd medical-telegram-warehouse
pip install -r requirements.txt

# Set required environment variables in .env
# DATABASE_URL, DB_HOST, DB_USER, DB_PASSWORD, DB_PORT, DB_NAME, DB_SSLMODE
# API_ID, API_HASH, PHONE_NUMBER for Telegram scraping

# Run the API locally
uvicorn api.main:app --reload

# Optional: run the full pipeline orchestration
dagster dev -f dagster_project/definitions.py
```

## Project Structure
```text
medical-telegram-warehouse/
├── api/                 # FastAPI app, routers, and response schemas
├── dagster_project/     # Pipeline definitions, resources, and assets
├── medical_warehouse/   # dbt project, staging models, marts, and tests
├── src/                 # Scraper, raw loader, and YOLO enrichment scripts
├── data/                # Raw JSON, downloaded images, and derived outputs
├── dashboard/           # Dashboard assets and UI experiments
├── tests/               # Automated tests for config and API behavior
└── README.md
```

## Demo
Dashboard: http://127.0.0.1:8501/ when the local dashboard app is running.

API docs: http://127.0.0.1:8000/docs

## Technical Details
   - Data: Telegram messages are scraped into daily JSON files, with associated images stored under `data/raw/images/`, then loaded into Postgres and modeled with dbt.
   - Model: Image enrichment uses YOLO-based detection to extract product-related signals from channel images before warehouse loading.
   - Evaluation: Data quality is validated with dbt tests and repository tests, while API routes expose search and report endpoints for downstream verification.

## Future Improvements
With more time, I would add scheduled production deployment, richer dashboard views, automated alerting for high-volume channels, improved evaluation for image detections, and stronger historical trend analysis across channels and dates.

## Author
Faysal Seifu

LinkedIn: https://www.linkedin.com/in/faysal-seifu-038443297/
