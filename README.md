# pallisade-airflow

Apache Airflow repo for orchestrating your BigQuery + dbt pipeline.

## What this orchestrates (based on your lineage)

From the lineage graph in Pallisade:
- `bigen-484520.analytics.stg_orders` depends on `bigen-484520.raw.orders`
- `bigen-484520.analytics.daily_revenue` depends on `bigen-484520.raw.orders`, `bigen-484520.raw.customers`, and `bigen-484520.analytics.stg_orders`
- `bigen-484520.analytics.product_performance`, `...customer_metrics`, and `...int_customer_orders` are downstream of `...stg_orders`

This repo includes an orchestration DAG:
- `dags/pipeline_dbt_bigquery.py`

It runs dbt for the lineage-relevant models: `stg_orders+ daily_revenue product_performance customer_metrics int_customer_orders`.

## Local development

### Start with Docker Compose (repo-native)

```bash
docker compose up airflow-init
docker compose up
```

Airflow UI:
- http://localhost:8081

Postgres (optional):
- localhost:5433

### Configure dbt for the Airflow container

The orchestration DAG expects your dbt project to be available at:
- `/opt/airflow/dbt`

You have two options:
1) **Mount your dbt project** into the container at runtime
2) **Vendor the dbt project** into this repo under `dbt/` and mount it in `docker-compose.yml`

Also set these variables (defaults shown):
- `DBT_PROJECT_DIR=/opt/airflow/dbt`
- `DBT_PROFILES_DIR=/opt/airflow/dbt`
- `DBT_TARGET=prod`
- `DBT_SELECTOR="stg_orders+ daily_revenue product_performance customer_metrics int_customer_orders"`

### BigQuery credentials

For local runs, the simplest approach is to use Application Default Credentials (ADC) and mount them into the container.

Example (host):
```bash
gcloud auth application-default login
gcloud config set project bigen-484520
```

Then ensure the ADC file is mounted into the Airflow containers (see your local docker-compose customization).

## Defaults

Airflow admin user (created by `airflow-init`):
- username: `airflow`
- password: `airflow`
