# Multi-Agency Asteroid Research Platform

A Docker-based research platform for collecting, processing, storing, and visualizing Near-Earth Object data from NASA/JPL and ESA sources.

The project has five main pieces:

- `Kafka` receives asteroid messages from producers.
- `Spark` consumers process Kafka messages and write normalized records.
- `TimescaleDB` stores close approaches, agency profiles, history tables, and analytics views.
- `FastAPI` serves the frontend and research API endpoints.
- `Next.js` and `Grafana` provide the 3D interface and analytics dashboard.

## Prerequisites

Install these first:

- Docker Desktop with Docker Compose.
- Git.
- A NASA API key from `https://api.nasa.gov/`.
- At least 8 GB RAM available for Docker. More is better for Spark.

You do not need to install Python, Java, Spark, Kafka, Postgres, Grafana, Node, or Bun on your host machine. Docker provides them.

## Fresh Setup

Clone the project and enter the folder:

```powershell
git clone <your-repository-url>
cd neo-project
```

Create your environment file:

```powershell
Copy-Item .env.example .env
```

Open `.env` and set at least:

```env
NASA_API_KEYS=YOUR_NASA_KEY
```

You can provide multiple NASA keys separated by commas:

```env
NASA_API_KEYS=KEY1,KEY2,KEY3
```

## Start The Stack

Pull images and start the base services:

```powershell
docker compose pull
docker compose up -d --build
```

Check that containers are running:

```powershell
docker compose ps
```

Wait until `kafka`, `timescaledb`, `neo-api`, `neo-frontend`, and `grafana` are up. The first run can take a few minutes because images and frontend dependencies are downloaded.

## Database Setup

On a fresh Docker volume, TimescaleDB automatically runs:

- `src/db/schema.sql`
- `src/db/history_init.sql`

If you already had a database volume and pulled new code, manually apply the schema updates:

```powershell
Get-Content src/db/schema.sql | docker exec -it timescaledb psql -U neo_user -d neo_db
Get-Content src/db/history_init.sql | docker exec -it timescaledb psql -U neo_user -d neo_db
```

To confirm the database is reachable:

```powershell
docker exec -it timescaledb psql -U neo_user -d neo_db -c "SELECT COUNT(*) FROM neo_close_approaches;"
```

## Run The Pipeline

Open four terminals from the project folder.

Terminal 1: process NASA NeoWs close approaches.

```powershell
docker exec -it neo-app python -m src.consumer.spark_processor
```

Terminal 2: fetch NASA NeoWs close approach data.

```powershell
docker exec -it neo-app python -m src.producer.neo_producer
```

Terminal 3: process multi-agency profile data.

```powershell
docker exec -it neo-app python -m src.consumer.agency_processor
```

Terminal 4: fetch multi-agency enrichment data.

```powershell
docker exec -it neo-app python -m src.producer.agency_producer
```

Recommended order is consumers first, then producers. That way Kafka messages are processed as soon as they arrive.

## Open The Apps

- Frontend 3D app: `http://localhost:3000`
- Grafana dashboard: `http://localhost:3001`
- FastAPI docs: `http://localhost:8000/docs`
- API health check: `http://localhost:8000/health`

Default Grafana login:

```text
Username: admin
Password: admin
```

Change this in `.env` for shared or public environments:

```env
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=your_strong_password
```

## Data Flow

The normal flow is:

```text
NASA NeoWs API -> neo_producer -> Kafka topic neows_ingest
Kafka topic neows_ingest -> spark_processor -> TimescaleDB
TimescaleDB asteroid list -> agency_producer -> Kafka topic agency_ingest
Kafka topic agency_ingest -> agency_processor -> TimescaleDB agency tables
TimescaleDB -> FastAPI, Grafana, Next.js frontend
```

## Data Sources

The project uses these external sources:

- NASA NeoWs feed for close approach events.
- JPL SBDB for orbital and physical parameters.
- JPL CAD for close approach history.
- JPL Sentry for impact risk objects.
- ESA NEOCC risk list.

## SBDB Missing Data Behavior

Older objects, especially historical records around years like 1899 or 1904, may not exist in every external database. This is normal.

If SBDB returns `404`, the agency producer does not stop. It continues checking the other APIs:

- JPL Sentry
- JPL CAD
- ESA NEOCC

The producer also tries multiple identifiers for each asteroid before giving up on a source:

- `asteroid_id`
- `neo_reference_id`
- cleaned designation from `name`, for example `12345 (1998 AA)` becomes `1998 AA`
- raw `name`

Confirmed `404` results are stored in `neo_agency_fetch_status`. The same missing source is held for `AGENCY_NOT_FOUND_HOLD_DAYS` days so the system does not repeatedly call an API for a record known to be absent.

This means partial profiles are expected and useful. A missing SBDB row can still have Sentry, CAD, or ESA data.

## Important Environment Settings

These are the most useful settings in `.env`:

```env
START_DATE=1900-01-01
NASA_API_KEYS=YOUR_NASA_KEY

AGENCY_BATCH_LIMIT=2000
AGENCY_CONCURRENCY=25
AGENCY_SENTRY_SEED_LIMIT=500
AGENCY_NOT_FOUND_HOLD_DAYS=30
NEOWS_NOT_FOUND_HOLD_DAYS=30

GRAFANA_PORT=3001
NEXT_PUBLIC_API_URL=http://localhost:8000
```

Use a later `START_DATE`, such as `2015-01-01`, for a fast first run. Use `1900-01-01` for a deeper historical research run.

## Useful Commands

View logs:

```powershell
docker compose logs -f neo-api
docker compose logs -f neo-frontend
docker compose logs -f grafana
```

Open a shell in the Python app container:

```powershell
docker exec -it neo-app bash
```

Count stored close approaches:

```powershell
docker exec -it timescaledb psql -U neo_user -d neo_db -c "SELECT COUNT(*) FROM neo_close_approaches;"
```

Count agency records:

```powershell
docker exec -it timescaledb psql -U neo_user -d neo_db -c "SELECT 'sbdb' AS source, COUNT(*) FROM neo_agency_sbdb UNION ALL SELECT 'sentry', COUNT(*) FROM neo_agency_sentry UNION ALL SELECT 'cad', COUNT(*) FROM neo_agency_cad UNION ALL SELECT 'esa', COUNT(*) FROM neo_agency_esa;"
```

Check confirmed missing external records:

```powershell
docker exec -it timescaledb psql -U neo_user -d neo_db -c "SELECT source, status_code, COUNT(*) FROM neo_agency_fetch_status GROUP BY source, status_code ORDER BY source, status_code;"
```

## Troubleshooting

Docker cannot connect:

```text
failed to connect to the docker API
```

Start Docker Desktop and run the command again.

Grafana opens but dashboard has no data:

- Make sure the producers and consumers are running.
- Check TimescaleDB has records.
- Apply `src/db/schema.sql` if this is an existing volume.

Frontend loads but API calls fail:

- Check `neo-api` is running.
- Open `http://localhost:8000/health`.
- Confirm `NEXT_PUBLIC_API_URL=http://localhost:8000` in `.env`.

Agency processor looks idle:

- Start `src.producer.agency_producer`.
- Keep `AGENCY_SENTRY_SEED_LIMIT` above `0`.
- Check `neo_close_approaches` has rows.
- Remember that SBDB 404s are held and skipped, while other APIs continue.

NASA rate limits:

- Use a real NASA API key.
- Add multiple keys in `NASA_API_KEYS`.
- Reduce `AGENCY_CONCURRENCY` if external APIs are unstable.

## Reset Everything

This deletes database, Kafka, and Grafana volumes:

```powershell
docker compose down -v
docker compose up -d --build
```

Use this only when you want a clean start.

## Project Structure

```text
neo-project/
  docker-compose.yml
  Dockerfile
  .env.example
  src/
    api/                  FastAPI backend
    producer/             NeoWs and agency producers
    consumer/             Spark consumers
    db/                   TimescaleDB schema and history tables
    config.py             Shared configuration
    logger.py             Logging setup
  frontend/               Next.js 3D app
  grafana/                Grafana provisioning and dashboard JSON
  logs/                   Runtime logs
```

## Research Notes

The project keeps history tables for scientific traceability. When records change, older values are archived in `*_history` tables with a `change_type`. Grafana reads from stable analytics views such as `neo_daily_metrics` and `neo_processed`, which makes the dashboard usable while the raw agency coverage grows over time.
