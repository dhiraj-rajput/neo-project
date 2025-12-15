# NASA Near-Earth Object (NEO) Analysis Pipeline

A real-time data pipeline that fetches, processes, and visualizes NASA Near-Earth Object (NEO) data using Kafka, Spark Structured Streaming, TimescaleDB, and Streamlit.

## 🚀 Features

*   **Data Ingestion**: Fetches NEO data from NASA's API and streams it to Kafka.
*   **Real-time Processing**: Spark Structured Streaming processes raw data, calculates risk scores, and detects anomalies.
*   **Storage**: TimescaleDB (PostgreSQL) stores both raw and processed data for efficient time-series querying.
*   **Visualization**:
    *   **Streamlit Dashboard**: Interactive UI to explore asteroids, risk levels, and daily metrics.
    *   **Grafana**: For monitoring system metrics (optional).

## 🛠️ Architecture

1.  **Fetch Service** (`fetch_raw.py`): Queries NASA API -> Produces to Kafka Topic `neo-raw-data`.
2.  **Spark Processor** (`spark_processor.py`): Consumes from Kafka -> Calculates Risk Score -> Writes to `neo_processed` table in DB.
3.  **Metrics Aggregator** (`calculate_metrics_data.py`): Aggregates processed data into daily metrics -> Writes to `neo_daily_metrics` table.
4.  **UI** (`streamlit_ui.py`): Reads from DB -> Displays Dashboard.

## 📋 Prerequisites

*   Docker and Docker Compose
*   NASA API Key (Get one [here](https://api.nasa.gov/))

## 🏁 Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd neo-project
```

### 2. Configure Environment

Copy the example environment file and add your NASA API key:

```bash
cp .env.example .env
```

Edit `.env` and set `NASA_API_KEY=your_actual_key`.

### 3. Start Services

Start the infrastructure (Kafka, TimescaleDB, Grafana) and application containers:

```bash
docker-compose up -d
```

### 4. Run the Pipeline

The application containers (`neo-app` and `neo-ui`) start in a waiting state. You need to execute the scripts manually to start processing.

**Step 1: Start Data Fetching**
```bash
docker-compose exec neo-app python fetch_raw.py
```
*This will fetch data for the configured date range and send it to Kafka.*

**Step 2: Start Spark Processor**
Open a new terminal and run:
```bash
docker-compose exec neo-app python spark_processor.py
```
*This starts the streaming job. Keep this terminal open.*

**Step 3: Start Metrics Aggregator**
Open a new terminal and run:
```bash
docker-compose exec neo-app python calculate_metrics_data.py
```
*This continuously calculates daily statistics.*

**Step 4: Launch Streamlit Dashboard**
Open a new terminal and run:
```bash
docker-compose exec neo-ui streamlit run streamlit_ui.py
```

## 📊 Accessing the Dashboards

*   **Streamlit UI**: [http://localhost:8501](http://localhost:8501)
*   **Grafana**: [http://localhost:3000](http://localhost:3000) (Default login: `admin` / `admin`)

## 📁 Project Structure

*   `fetch_raw.py`: Data ingestion script.
*   `spark_processor.py`: Spark Structured Streaming logic.
*   `calculate_metrics_data.py`: Daily metrics aggregation.
*   `streamlit_ui.py`: Dashboard application.
*   `db-init.sql`: Database initialization script.
*   `docker-compose.yml`: Container orchestration.

## 📝 License

[MIT](LICENSE)
