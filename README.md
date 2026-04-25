# medical-telegram-warehouse

Project scaffold for a FastAPI service with a dbt warehouse layer.

## Quickstart
1. Create a `.env` file with your secrets (DB URL, API keys). Do not commit it.
   - For the Neon database, set `DATABASE_URL=postgresql://neondb_owner:npg_Q5NtksMJCTA7@ep-late-silence-ah63hd3s-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require`
   - Also set the matching dbt variables: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_PORT`, `DB_NAME`, `DB_SSLMODE`
2. Build and run the API:
   - `docker-compose up --build`
3. Run the dashboard locally against the API:
   - `pip install -r requirements.txt`
   - `set API_BASE_URL=http://localhost:8000`
   - `streamlit run streamlit_app.py`
4. Run tests locally:
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
- Use the Neon database from `.env` for the app, loaders, and dbt.
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
  - Use a Python 3.11 runtime for dbt 1.8; the current Python 3.14 venv in this workspace cannot start the dbt CLI because of protobuf/upb incompatibility.
- Launch Dagster UI:
   - `dagster dev -f dagster_project/definitions.py`
- In the UI, materialize the full pipeline job `daily_full_pipeline` or trigger individual assets:
   - `raw_telegram_data` → `raw_postgres_load` → `yolo_image_detections` → `yolo_csv_to_postgres` → `dbt_transforms`

## Task 6: Interactive Dashboard
- Start the FastAPI app and then open the Streamlit front end:
  - `streamlit run streamlit_app.py`
- The dashboard is organized into four batch-aware views:
  - Overview
  - Model predictions
  - Business impact
  - Drill-down
- Freshness is reported from the latest warehouse refresh timestamp surfaced by the API, so the UI reflects the last successful pipeline run instead of simulating live streaming.
