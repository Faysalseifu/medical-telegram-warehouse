import subprocess

from dagster import asset, AssetExecutionContext, AssetIn


@asset(ins={"raw_telegram_data": AssetIn()})
def raw_postgres_load(context: AssetExecutionContext) -> str:
    """Load raw Telegram JSON into Postgres (raw.telegram_messages)."""
    context.log.info("Loading raw data to Postgres via src/load_raw.py ...")

    result = subprocess.run([
        "python",
        "src/load_raw.py",
    ], capture_output=True, text=True, check=False)

    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Raw load failed with code {result.returncode}")

    return "raw.telegram_messages loaded"
