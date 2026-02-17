# pallisade-airflow

Apache Airflow repository for orchestrating the **Pallisade BigQuery + dbt pipeline**.

This repo is the **orchestration layer** of your stack:

- **BigQuery** (warehouse): stores raw + analytics datasets in project `bigen-484520`
- **dbt Core** (transformations): defines the SQL models/views/tables inside BigQuery
- **Airflow (this repo)**: schedules and runs dbt reliably (retries, logs, alerting hooks), so the warehouse stays fresh
- **Pallisade** (reliability/observability): tracks lineage and health across the pipeline

---

## How Airflow fits into *your* pipeline

### What Airflow does

Airflow is responsible for:

1. **Scheduling** a daily pipeline run (cron)
2. **Executing dbt commands** to build the analytics layer
3. Providing operational controls: retries, run history, task-level logs, and an execution “source of truth”

In this setup, Airflow is not where transformations are written—**dbt is**. Airflow’s job is to make sure dbt runs at the right time, in the right environment, with the right credentials.

### What Airflow does *not* do

- It does **not** ingest source data into `bigen-484520.raw` (ingestion is upstream of this repo)
- It does **not** replace dbt tests—rather it *runs* them (via `dbt build`)
- It does **not** store analytics results itself; those live in BigQuery

---

## What this orchestrates (based on your Pallisade lineage)

From the lineage graph in Pallisade, these are key dependencies and outputs:

### Upstream sources (BigQuery)
- `bigen-484520.raw.orders`
- `bigen-484520.raw.customers`
- `bigen-484520.raw.events`
- `bigen-484520.raw.products`

### Staging (dbt models/views in BigQuery)
- `bigen-484520.analytics.stg_orders` (from `bigen-484520.raw.orders`)
- `bigen-484520.analytics.stg_customers` (from `bigen-484520.raw.customers`)
- `bigen-484520.analytics.stg_events` (from `bigen-484520.raw.events`)
- `bigen-484520.analytics.stg_products` (from `bigen-484520.raw.products`)

### Intermediate + marts (analytics outputs)
- `bigen-484520.analytics.int_customer_orders`
- `bigen-484520.analytics.int_customer_events`
- `bigen-484520.analytics.daily_revenue`
- `bigen-484520.analytics.product_performance`
- `bigen-484520.analytics.customer_metrics`
- `bigen-484520.analytics.conversion_funnel`

In short:

`raw.*` → `analytics.stg_*` → `analytics.int_*` → `analytics.* (marts)`

Airflow ensures the dbt graph that produces these assets is executed on schedule.

---

## DAGs in this repo

### `pallisade_dbt_bigquery_pipeline`

- **DAG file:** `dags/pipeline_dbt_bigquery.py`
- **Schedule:** daily at **06:00 UTC**
- **Purpose:** run dbt to build and test the analytics layer in BigQuery

Tasks:

1. `dbt_deps`
   - Runs: `dbt deps`
   - Why: ensures dbt packages are installed before building

2. `dbt_build`
   - Runs: `dbt build` (dbt run + dbt test)
   - Why: builds models and executes associated dbt tests so data quality failures fail the run

Model selection:

- Default selector (can be overridden):
  - `stg_orders+ daily_revenue product_performance customer_metrics int_customer_orders`

Override with environment variable:

- `DBT_SELECTOR` — use this to broaden or narrow what the DAG builds.

---

## Configuration

### dbt project location inside the Airflow container

The DAG expects your dbt project to be available at:

- `/opt/airflow/dbt`

Two supported approaches:

1. **Mount your dbt project** into the Airflow container at runtime
2. **Vendor the dbt project** into this repository (e.g. under `dbt/`) and mount it in `docker-compose.yml`

Relevant environment variables (defaults shown):

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

Ensure the ADC file is mounted into the Airflow containers (via your local Docker Compose customization).

---

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

---

## Operational notes

- If downstream tables like `bigen-484520.analytics.daily_revenue` are stale, the first place to check is the Airflow DAG run history and the `dbt_build` task logs.
- If a dbt test fails, the DAG run will fail (by design). Fix the failing model/test in the dbt repo, then re-run the DAG.

---

## Defaults

Airflow admin user (created by `airflow-init`):

- username: `airflow`
- password: `airflow`
