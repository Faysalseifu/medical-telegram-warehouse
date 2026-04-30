# Medical Telegram Warehouse

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688)](https://fastapi.tiangolo.com/)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694B)](https://www.getdbt.com/)
[![Dagster](https://img.shields.io/badge/Dagster-1.8-5A6ACF)](https://dagster.io/)
[![YOLOv8](https://img.shields.io/badge/YOLOv8-ultralytics-111111)](https://docs.ultralytics.com/)

Portfolio-ready Telegram ELT pipeline that ingests medical channel content, enriches images with YOLOv8, models a star schema in dbt, and exposes analytics through a FastAPI service orchestrated by Dagster.

## Project Overview
- **Goal:** Build an end-to-end ELT pipeline for Telegram medical content and make it analytics-ready.
- **Inputs:** Telegram messages + images from curated channels.
- **Outputs:** PostgreSQL warehouse (raw, staging, marts), enriched YOLO detections, and API endpoints for reports.

## Tech Stack
- **Ingestion:** Telethon
- **Warehouse:** PostgreSQL + dbt
- **Enrichment:** YOLOv8 (Ultralytics)
- **API:** FastAPI + SQLAlchemy
- **Orchestration:** Dagster
- **Infra:** Docker Compose

## Quick Start
1. Create `.env` from `.env.example` and fill in Telegram and database values.
2. Install dependencies:
   ```bash
   python -m venv .venv
   source .venv/Scripts/activate
   pip install -r requirements.txt
   ```
3. Run the scraper (raw JSON + images):
   ```bash
   python src/scraper.py
   ```
4. Load raw data into Postgres:
   ```bash
   python src/load_raw.py
   ```
5. Run dbt transformations:
   ```bash
   cd medical_warehouse
   dbt run --select staging marts
   dbt test
   ```
6. Start the API:
   ```bash
   uvicorn api.main:app --reload
   ```

Optional: run the API via Docker Compose:
```bash
docker-compose up --build
```

## Architecture Summary
```mermaid
flowchart LR
    A[Telegram channels] -->|Telethon| B[Raw JSON + images]
    B --> C[raw.telegram_messages]
    B -->|YOLOv8| D[raw.yolo_detections]
    C --> E[stg_telegram_messages]
    E --> F[dim_channels]
    E --> G[dim_dates]
    E --> H[fct_messages]
    D --> I[fct_image_detections]
    H --> J[FastAPI analytics]
    I --> J
    subgraph Orchestration
      K[Dagster assets]
    end
    K --> C
    K --> D
    K --> E
```

## Repository Structure
- `api/` FastAPI application and routers
- `src/` Scraper, raw loader, YOLO inference
- `medical_warehouse/` dbt project (staging + marts)
- `dagster_project/` Dagster assets and resources
- `data/` Raw and enriched artifacts
- `docs/` Data dictionary and diagrams

## Future Improvements
- Add incremental loads for raw and staging layers
- Track schema changes with dbt exposures and docs site
- Add topic modeling for message text
- Enrich detections with more domain-specific classes
- Add CI for dbt + API tests
