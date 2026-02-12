"""Example DAG for the pallisade-airflow repository.

This DAG is intentionally simple and demonstrates parse-safe patterns:
- No network calls at import time
- Static start_date
- catchup disabled

Replace this with your actual orchestration (dbt runs, warehouse jobs, etc.).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator


DEFAULT_ARGS = {
    "owner": "pallisade",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="example_pallisade_daily_revenue",
    description="Example scaffold DAG (replace with real pipelines)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["pallisade", "example"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
