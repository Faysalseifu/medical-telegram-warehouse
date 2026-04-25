import subprocess

from pathlib import Path

from dagster import asset, AssetExecutionContext

from ..utils import run_python_script


@asset
def raw_telegram_data(context: AssetExecutionContext) -> Path:
    """Run the Telegram scraper to fetch latest messages and images."""
    result = run_python_script(context, Path("src/scraper.py"), "Telegram scrape")
    if result.returncode != 0:
        raise RuntimeError(f"Scraper failed with code {result.returncode}")

    out_path = Path("data/raw/telegram_messages")
    context.log.info(f"Scrape complete. Raw messages at: {out_path}")
    return out_path
