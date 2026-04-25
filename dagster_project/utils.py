from __future__ import annotations

import subprocess
from pathlib import Path

from dagster import AssetExecutionContext


def run_python_script(
    context: AssetExecutionContext,
    script_path: str | Path,
    description: str,
) -> subprocess.CompletedProcess[str]:
    context.log.info("Running %s via %s", description, script_path)
    result = subprocess.run(["python", str(script_path)], capture_output=True, text=True, check=False)

    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)

    return result