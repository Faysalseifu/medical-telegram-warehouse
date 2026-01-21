from pathlib import Path

from dagster_dbt import DbtCliResource

# Point to the dbt project and profiles
DBT_PROJECT_DIR = Path(__file__).parent.parent / "medical_warehouse"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Dagster resource for running dbt commands
# Ensure that a manifest is available by running `dbt compile` once.
dbt = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)
