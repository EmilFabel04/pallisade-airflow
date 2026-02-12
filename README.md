# pallisade-airflow

Apache Airflow repo scaffold for Pallisade.

## Local development

### Start with Docker Compose (repo-native)

This repo includes a `docker-compose.yml` that runs:
- Postgres (metadata DB)
- Airflow webserver
- Airflow scheduler

Bring it up:

```bash
docker compose up airflow-init
docker compose up
```

Airflow UI:
- http://localhost:8081 (mapped from container port 8080)

Postgres (optional, for connecting with a DB client):
- localhost:5433 (mapped from container port 5432)

> Note: Host ports are remapped via `docker-compose.override.yml` to avoid conflicts if you already have services on 8080/5432.

### Start with Astronomer (Astro)

If you use Astronomer CLI, you can still keep this repo structure, but ensure your Astro project uses non-default host ports.

If you hit a port conflict on startup, make sure `docker-compose.override.yml` exists and then restart:

```bash
astro dev stop
astro dev start
```

## Defaults

Airflow admin user (created by `airflow-init`):
- username: `airflow`
- password: `airflow`
