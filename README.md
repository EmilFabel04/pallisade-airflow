# pallisade-airflow

Apache Airflow DAG repository for Pallisade.

## Repository layout

- `dags/` — Airflow DAG definitions
- `plugins/` — Custom operators/hooks/sensors (if needed)
- `include/` — SQL/templates/config files used by DAGs
- `tests/` — Unit tests for DAG import/structure

## Local development (Docker Compose)

Prereqs: Docker + Docker Compose

```bash
# From repo root
cp .env.example .env

docker compose up --build
```

Airflow UI: http://localhost:8080  
Default credentials: `airflow` / `airflow`

## Adding a DAG

1. Create a new file in `dags/` (e.g. `dags/my_dag.py`).
2. Keep `start_date` static and in the past (don’t use `datetime.now()`), and set `catchup=False` unless you explicitly need backfills.
3. Import must succeed with no network calls at parse time.

## CI expectations

- DAGs must import successfully.
- Basic lint/format checks should pass.
