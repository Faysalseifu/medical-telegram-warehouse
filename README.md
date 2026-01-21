# medical-telegram-warehouse

Project scaffold for a FastAPI service with a dbt warehouse layer.

## Quickstart
1. Create a `.env` file with your secrets (DB URL, API keys). Do not commit it.
2. Build and run the API:
   - `docker-compose up --build`
3. Run tests locally:
   - `pip install -r requirements.txt`
   - `pytest`

## Structure
- API service in `api/`
- dbt project in `medical_warehouse/`
- Notebooks and scripts in `notebooks/` and `scripts/`

## Task 1: Telegram Scraper
- Install deps: `pip install -r requirements.txt`
- Add to `.env`: `API_ID`, `API_HASH`, `PHONE_NUMBER`
- Run scraper: `python src/scraper.py`
- Outputs:
  - JSON: `data/raw/telegram_messages/YYYY-MM-DD/<channel>.json`
  - Images: `data/raw/images/<channel>/<message_id>.jpg`

## Task 2: Load + Transform
- Start Postgres (local or remote). For local Docker:
   - `docker compose up -d postgres`
- Load raw JSON to Postgres:
   - `python src/load_raw.py`
- dbt (from `medical_warehouse/`):
   - `dbt debug`
   - `dbt run --select staging marts`
   - `dbt test`

## Task 3: YOLO Enrichment
- Run detections to generate CSV:
   - `python src/yolo_detect.py`
- Optional: load detections into Postgres:
   - Handled by Dagster asset `yolo_csv_to_postgres` or via manual SQL COPY.

## Task 5: Orchestration (Dagster)
- Install Dagster deps:
   - `pip install -r requirements.txt`
- Ensure dbt manifest exists:
   - `cd medical_warehouse && dbt compile`
- Launch Dagster UI:
   - `dagster dev -f dagster_project/definitions.py`
- In the UI, materialize the full pipeline job `daily_full_pipeline` or trigger individual assets:
   - `raw_telegram_data` → `raw_postgres_load` → `yolo_image_detections` → `yolo_csv_to_postgres` → `dbt_transforms`
