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

> Note: ports are remapped directly in `docker-compose.yml` so it works even if you already have services on 8080/5432.

### Start with Astronomer (Astro)

This repo includes an Astro `Dockerfile` (Astronomer Runtime). If you run:

```bash
astro dev start
```

Astro may still try to bind default local ports depending on your local Astro config. If you hit port conflicts, the easiest path is to run the repo-native compose above.

If you prefer Astro for local dev, run it on the same free ports by using a compose file override (recommended):

1) Create `compose.local.yml`:
```yaml
services:
  postgres:
    ports:
      - "127.0.0.1:5433:5432"
  airflow-webserver:
    ports:
      - "127.0.0.1:8081:8080"
```

2) Start Astro using it:
```bash
astro dev stop || true
astro dev start --compose-file compose.local.yml
```

## Defaults

Airflow admin user (created by `airflow-init`):
- username: `airflow`
- password: `airflow`
