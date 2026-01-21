from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource

from ..resources import dbt

# Use the compiled manifest.json from the dbt project's target directory
MANIFEST_PATH = Path(__file__).parent.parent.parent / "medical_warehouse" / "target" / "manifest.json"


@dbt_assets(manifest=MANIFEST_PATH)
def dbt_transforms(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run full dbt build, exposing dbt models as assets."""
    # Stream dbt logs/results into Dagster
    yield from dbt.cli(["build"], context=context).stream()
