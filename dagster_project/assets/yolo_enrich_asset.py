import subprocess
from pathlib import Path

from dagster import asset, AssetExecutionContext, AssetIn


@asset(ins={"raw_postgres_load": AssetIn()})
def yolo_image_detections(context: AssetExecutionContext) -> Path:
    """Run YOLO on images to produce enriched detections CSV."""
    context.log.info("Running YOLO enrichment via src/yolo_detect.py ...")

    result = subprocess.run([
        "python",
        "src/yolo_detect.py",
    ], capture_output=True, text=True, check=False)

    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"YOLO enrichment failed with code {result.returncode}")

    out_csv = Path("data/enriched/yolo_detections.csv")
    context.log.info(f"YOLO detections CSV at: {out_csv}")
    return out_csv
