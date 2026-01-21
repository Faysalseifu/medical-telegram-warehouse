from pathlib import Path
import os

import pandas as pd
from dagster import asset, AssetExecutionContext, AssetIn
from dotenv import load_dotenv
from sqlalchemy import create_engine


@asset(ins={"yolo_image_detections": AssetIn()})
def yolo_csv_to_postgres(context: AssetExecutionContext) -> str:
    """Load YOLO detections CSV into Postgres table raw.yolo_detections."""
    load_dotenv()
    database_url = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:password@localhost:5432/postgres")
    engine = create_engine(database_url, future=True)

    csv_path = Path("data/enriched/yolo_detections.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found at {csv_path}")

    df = pd.read_csv(csv_path)

    # Ensure table exists with expected schema
    with engine.begin() as conn:
        conn.execute(
            """
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                image_path VARCHAR,
                channel_name VARCHAR,
                message_id BIGINT,
                category VARCHAR(50),
                max_confidence FLOAT,
                detections TEXT,
                processed_at TIMESTAMP
            );
            """
        )

    df.to_sql(
        name="yolo_detections",
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
    )

    context.log.info(f"Loaded {len(df)} records into raw.yolo_detections")
    return "raw.yolo_detections loaded"
