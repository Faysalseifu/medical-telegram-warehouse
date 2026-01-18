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
