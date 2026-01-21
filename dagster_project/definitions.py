from dagster import Definitions, ScheduleDefinition, AssetSelection

from .assets.scraper_asset import raw_telegram_data
from .assets.raw_load_asset import raw_postgres_load
from .assets.yolo_enrich_asset import yolo_image_detections
from .assets.yolo_load_asset import yolo_csv_to_postgres
from .assets.dbt_assets import dbt_transforms
from .resources import dbt

all_assets = [
    raw_telegram_data,
    raw_postgres_load,
    yolo_image_detections,
    yolo_csv_to_postgres,
    dbt_transforms,
]

# Define a simple daily schedule that runs the full pipeline
full_pipeline_job = AssetSelection.assets(
    raw_telegram_data,
    raw_postgres_load,
    yolo_image_detections,
    yolo_csv_to_postgres,
    dbt_transforms,
).to_job(name="daily_full_pipeline")

full_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    name="daily_pipeline_schedule",
)


defs = Definitions(
    assets=all_assets,
    resources={"dbt": dbt},
    schedules=[full_pipeline_schedule],
)
