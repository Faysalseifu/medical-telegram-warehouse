from pathlib import Path
import subprocess

from dagster import asset, AssetExecutionContext


@asset
def raw_telegram_data(context: AssetExecutionContext) -> Path:
    """Run the Telegram scraper to fetch latest messages and images."""
    context.log.info("Starting Telegram scrape via src/scraper.py ...")

    result = subprocess.run([
        "python",
        "src/scraper.py",
    ], capture_output=True, text=True, check=False)

    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Scraper failed with code {result.returncode}")

    out_path = Path("data/raw/telegram_messages")
    context.log.info(f"Scrape complete. Raw messages at: {out_path}")
    return out_path
