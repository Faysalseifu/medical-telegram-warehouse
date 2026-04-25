from pathlib import Path

from dagster import asset, AssetExecutionContext, AssetIn

from ..utils import run_python_script


@asset(ins={"raw_postgres_load": AssetIn()})
def yolo_image_detections(context: AssetExecutionContext) -> Path:
    """Run YOLO on images to produce enriched detections CSV."""
    result = run_python_script(context, Path("src/yolo_detect.py"), "YOLO enrichment")
    if result.returncode != 0:
        raise RuntimeError(f"YOLO enrichment failed with code {result.returncode}")

    out_csv = Path("data/enriched/yolo_detections.csv")
    context.log.info(f"YOLO detections CSV at: {out_csv}")
    return out_csv
