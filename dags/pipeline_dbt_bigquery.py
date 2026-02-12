"""Pallisade pipeline orchestration (dbt -> BigQuery)

This DAG is based on your current lineage:
- Upstream sources: `bigen-484520.raw.orders`, `bigen-484520.raw.customers`
- Staging: `bigen-484520.analytics.stg_orders`
- Downstream marts: `bigen-484520.analytics.daily_revenue`, `...product_performance`,
  `...customer_metrics`, `...int_customer_orders`

It orchestrates dbt for those models:
- `dbt deps` (optional but safe)
- `dbt build` for the model set (runs + tests)

Assumptions / required setup:
- Your dbt project code is available inside the Airflow container at `/opt/airflow/dbt`.
  (Mount it there or vendor it into this repo.)
- BigQuery credentials are provided via ADC or a service account JSON.

Environment variables supported:
- DBT_PROJECT_DIR: default `/opt/airflow/dbt`
- DBT_PROFILES_DIR: default `/opt/airflow/dbt`
- DBT_TARGET: default `prod`
- DBT_SELECTOR: default `stg_orders+ daily_revenue product_performance customer_metrics int_customer_orders`

"""

from __future__ import annotations

import os
import shlex
import subprocess
from datetime import timedelta

from airflow.decorators import dag, task
from pendulum import datetime


def _run(cmd: str, env: dict[str, str] | None = None) -> str:
    """Run a shell command and return stdout.

    Raises RuntimeError with combined stdout/stderr for easier debugging in task logs.
    """

    proc = subprocess.run(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {cmd}\n\n{proc.stdout}")
    return proc.stdout


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # daily 06:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["pallisade", "dbt", "bigquery"],
)
def pallisade_dbt_bigquery_pipeline():
    dbt_project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR", dbt_project_dir)
    dbt_target = os.getenv("DBT_TARGET", "prod")

    # This matches the lineage-relevant models. You can replace with a dbt selector in your repo.
    default_selector = "stg_orders+ daily_revenue product_performance customer_metrics int_customer_orders"
    dbt_selector = os.getenv("DBT_SELECTOR", default_selector)

    dbt_env = {
        **os.environ,
        "DBT_PROJECT_DIR": dbt_project_dir,
        "DBT_PROFILES_DIR": dbt_profiles_dir,
    }

    @task
    def dbt_deps() -> str:
        return _run(
            f"dbt deps --project-dir {dbt_project_dir} --profiles-dir {dbt_profiles_dir}",
            env=dbt_env,
        )

    @task
    def dbt_build() -> str:
        # dbt build = run + test for selected nodes
        return _run(
            " ".join(
                [
                    "dbt",
                    "build",
                    "--project-dir",
                    dbt_project_dir,
                    "--profiles-dir",
                    dbt_profiles_dir,
                    "--target",
                    dbt_target,
                    "--select",
                    dbt_selector,
                ]
            ),
            env=dbt_env,
        )

    dbt_deps() >> dbt_build()


pallisade_dbt_bigquery_pipeline()
