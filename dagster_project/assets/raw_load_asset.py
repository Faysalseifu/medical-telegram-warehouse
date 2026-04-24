from pathlib import Path

from dagster import asset, AssetExecutionContext, AssetIn

from ..utils import run_python_script


@asset(ins={"raw_telegram_data": AssetIn()})
def raw_postgres_load(context: AssetExecutionContext) -> str:
    """Load raw Telegram JSON into Postgres (raw.telegram_messages)."""
    result = run_python_script(context, Path("src/load_raw.py"), "raw load")
    if result.returncode != 0:
        raise RuntimeError(f"Raw load failed with code {result.returncode}")

    return "raw.telegram_messages loaded"
