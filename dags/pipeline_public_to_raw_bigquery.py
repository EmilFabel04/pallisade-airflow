from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable

# BigQuery provider hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


PROJECT_ID = "bigen-484520"
SOURCE_DATASET = "public"
TARGET_DATASET = "raw"

# Use the standard conn id. For local docker you can provide it via env var:
# AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?extra__google_cloud_platform__project=bigen-484520'
# plus GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json (or configure keyfile json in the conn extra).
GCP_CONN_ID = "google_cloud_default"

TABLE_CONFIGS = [
    {
        "name": "customers",
        "pk": "id",
        "synced_col": "_fivetran_synced",
        "deleted_col": "_fivetran_deleted",
        "loaded_at_col": "_loaded_at",
        "soft_delete_col": "_deleted",
    },
    {
        "name": "orders",
        "pk": "id",
        "synced_col": "_fivetran_synced",
        "deleted_col": "_fivetran_deleted",
        "loaded_at_col": "_loaded_at",
        "soft_delete_col": "_deleted",
    },
    {
        "name": "events",
        "pk": "id",
        "synced_col": "_fivetran_synced",
        "deleted_col": "_fivetran_deleted",
        "loaded_at_col": "_loaded_at",
        "soft_delete_col": "_deleted",
    },
    {
        "name": "products",
        "pk": "id",
        "synced_col": "_fivetran_synced",
        "deleted_col": "_fivetran_deleted",
        "loaded_at_col": "_loaded_at",
        "soft_delete_col": "_deleted",
    },
]


def _var_key(table_name: str) -> str:
    return f"public_to_raw__{table_name}__last_fivetran_synced"


def _get_watermark(table_name: str) -> str:
    # Start at epoch to allow first run to pick up all currently available rows.
    return Variable.get(_var_key(table_name), default_var="1970-01-01T00:00:00Z")


def _set_watermark(table_name: str, new_value: str) -> None:
    Variable.set(_var_key(table_name), new_value)


def _ensure_soft_delete_column_sql(table_name: str, soft_delete_col: str) -> str:
    return f"""
DECLARE col_exists BOOL DEFAULT (
  SELECT COUNT(1) > 0
  FROM `{PROJECT_ID}.{TARGET_DATASET}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{table_name}'
    AND column_name = '{soft_delete_col}'
);

IF NOT col_exists THEN
  EXECUTE IMMEDIATE 'ALTER TABLE `{PROJECT_ID}.{TARGET_DATASET}.{table_name}` ADD COLUMN {soft_delete_col} BOOL';
END IF;
"""


def _ensure_loaded_at_column_sql(table_name: str, loaded_at_col: str) -> str:
    return f"""
DECLARE col_exists BOOL DEFAULT (
  SELECT COUNT(1) > 0
  FROM `{PROJECT_ID}.{TARGET_DATASET}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{table_name}'
    AND column_name = '{loaded_at_col}'
);

IF NOT col_exists THEN
  EXECUTE IMMEDIATE 'ALTER TABLE `{PROJECT_ID}.{TARGET_DATASET}.{table_name}` ADD COLUMN {loaded_at_col} TIMESTAMP';
END IF;
"""


def _get_delta_max_synced_sql(table_name: str, synced_col: str, watermark: str) -> str:
    return f"""
SELECT
  MAX({synced_col}) AS max_synced
FROM `{PROJECT_ID}.{SOURCE_DATASET}.{table_name}`
WHERE {synced_col} > TIMESTAMP('{watermark}')
"""


def _merge_sql(table_name: str, pk: str, synced_col: str, deleted_col: str, loaded_at_col: str, soft_delete_col: str, watermark: str) -> str:
    # For orders: public has DATETIME for created_at/updated_at but raw expects STRING; cast those.
    if table_name == "orders":
        select_expr = f"""
SELECT
  src.* EXCEPT({synced_col}, {deleted_col}, created_at, updated_at),
  CAST(src.created_at AS STRING) AS created_at,
  CAST(src.updated_at AS STRING) AS updated_at,
  CURRENT_TIMESTAMP() AS {loaded_at_col},
  CAST(src.{deleted_col} AS BOOL) AS {soft_delete_col}
"""
    else:
        select_expr = f"""
SELECT
  src.* EXCEPT({synced_col}, {deleted_col}),
  CURRENT_TIMESTAMP() AS {loaded_at_col},
  CAST(src.{deleted_col} AS BOOL) AS {soft_delete_col}
"""

    return f"""
MERGE `{PROJECT_ID}.{TARGET_DATASET}.{table_name}` AS tgt
USING (
  {select_expr}
  FROM `{PROJECT_ID}.{SOURCE_DATASET}.{table_name}` AS src
  WHERE src.{synced_col} > TIMESTAMP('{watermark}')
) AS src
ON tgt.{pk} = src.{pk}
WHEN MATCHED THEN
  UPDATE SET
    tgt = src
WHEN NOT MATCHED THEN
  INSERT ROW
"""


def replicate_public_to_raw() -> None:
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)

    for cfg in TABLE_CONFIGS:
        table = cfg["name"]
        pk = cfg["pk"]
        synced_col = cfg["synced_col"]
        deleted_col = cfg["deleted_col"]
        loaded_at_col = cfg["loaded_at_col"]
        soft_delete_col = cfg["soft_delete_col"]

        watermark = _get_watermark(table)

        hook.run(_ensure_loaded_at_column_sql(table, loaded_at_col))
        hook.run(_ensure_soft_delete_column_sql(table, soft_delete_col))
        hook.run(_merge_sql(table, pk, synced_col, deleted_col, loaded_at_col, soft_delete_col, watermark))

        max_synced = hook.get_first(_get_delta_max_synced_sql(table, synced_col, watermark))
        if max_synced and max_synced[0]:
            _set_watermark(table, max_synced[0].isoformat())


with DAG(
    dag_id="pipeline_public_to_raw_bigquery",
    description="Replicate Fivetran BigQuery landing dataset (public) into raw with incremental MERGE + soft deletes",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=timedelta(minutes=30),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bigquery", "fivetran", "raw"],
) as dag:
    from airflow.operators.python import PythonOperator

    replicate = PythonOperator(
        task_id="replicate_public_to_raw",
        python_callable=replicate_public_to_raw,
    )
