# NEO Orbital Tracker — Multi-Agency Comparative Analysis Engine

A real-time data pipeline and 3D visualization engine for Near-Earth Object (NEO) tracking. Ingests data from NASA's NeoWs API, processes it via Apache Spark Structured Streaming, stores it in TimescaleDB, and displays it in an interactive 3D orbital visualization built with React Three Fiber.

## Architecture

```
NASA NeoWs API → Kafka → Spark Streaming → TimescaleDB → FastAPI → Next.js 3D Frontend
```

| Component | Technology |
|-----------|-----------|
| Ingestion | Python Producer → Kafka |
| Processing | PySpark Structured Streaming |
| Storage | TimescaleDB (PostgreSQL) |
| API | FastAPI (Python) |
| Frontend | Next.js + React Three Fiber |
| Monitoring | Grafana |
| Container Runtime | Docker Compose |

## Prerequisites

- Docker and Docker Compose
- NASA API Key ([Get one here](https://api.nasa.gov/))
- Bun (for local frontend development)

## Getting Started

### 1. Clone and Configure

```bash
git clone <repository-url>
cd neo-project
cp .env.example .env
# Edit .env and set NASA_API_KEYS=your_key
```

### 2. Start All Services

```bash
docker-compose up -d --build
```

### 3. Run the Pipeline

```bash
# Start data ingestion (runs continuously with 30-min delta polling)
docker-compose exec neo-app python -m src.producer.neo_producer

# Start Spark stream processor (in a new terminal)
docker-compose exec neo-app python -m src.consumer.spark_processor
```

### 4. Access the Application

| Service | URL |
|---------|-----|
| 3D Frontend | [http://localhost:3000](http://localhost:3000) |
| FastAPI Docs | [http://localhost:8000/docs](http://localhost:8000/docs) |
| Grafana | [http://localhost:3000](http://localhost:3000) |

## Project Structure

```
neo-project/
├── src/
│   ├── api/           # FastAPI backend
│   │   └── main.py
│   ├── producer/      # Kafka data ingestion
│   │   └── neo_producer.py
│   ├── consumer/      # Spark stream processing
│   │   └── spark_processor.py
│   ├── db/            # Database schema & migrations
│   │   ├── schema.sql
│   │   └── history_init.sql
│   ├── config.py      # Centralized configuration
│   └── logger.py      # Logging setup
├── frontend/          # Next.js 3D visualization
│   └── src/
│       ├── app/       # Pages & styles
│       └── components/ # React Three Fiber components
├── grafana/           # Grafana provisioning
├── Dockerfile         # Python backend image (uv + PySpark)
├── docker-compose.yml # Full orchestration
└── requirements.txt   # Python dependencies
```

## Pipeline Behavior

The producer runs **continuously** with two modes:

1. **Full Backfill**: On first startup, fetches all historical NEO data from `START_DATE` to today.
2. **Delta Polling**: After backfill, polls NASA every **30 minutes** for updated data (today ± 14 days). A full re-scan triggers every **6 hours**.

The Spark consumer uses **UPSERT** logic — if NASA revises an asteroid's trajectory, the old data is archived to the history table and the main table is updated with the latest values.

## License

[MIT](LICENSE)
