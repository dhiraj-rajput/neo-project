Absolutely! I’ve formatted your detailed instructions into a clean **Markdown (`.md`) file** with proper headings, code blocks, and clear steps. You can copy and save it as `TESTING_GUIDE.md` or any preferred name.

````markdown
# Neo Project: Testing Guide

This guide walks you through testing the **Neo Project** step by step, assuming all services are running via `docker-compose`.

The project services include:

- `kafka`
- `timescaledb`
- `grafana`
- `neo-app` (data processing)
- `neo-ui` (dashboard)

All Python scripts are executed inside the `neo-app` (processing) or `neo-ui` (UI) containers. Filenames are referenced by functional names; adjust if local filenames differ.

---

## Step 0: Verify Docker Services Are Ready

1. Check all containers:

```bash
docker ps
````

* Ensure all containers are `healthy` (especially Kafka and TimescaleDB).

2. If any service is not healthy, check logs:

```bash
docker logs kafka
docker logs timescaledb
```

3. Verify database tables:

```bash
docker exec -it timescaledb psql -U neo_user -d neo_db
\dt
\q
```

* Expected tables: `neo_raw`, `neo_processed`, `neo_daily_metrics`

4. Access Grafana:

* Open [http://localhost:3000](http://localhost:3000)
* Default login: `admin/admin`

**Tip:** Restart any failed service:

```bash
docker-compose down && docker-compose up -d
```

---

## Step 1: Install Dependencies in Containers

### neo-app (processing)

```bash
docker exec -it neo-app bash
pip install -r requirements.txt
exit
```

### neo-ui (dashboard)

```bash
docker exec -it neo-ui bash
pip install -r requirements.txt
exit
```

Dependencies include:

* `requests`, `kafka-python`, `pyspark`, `psycopg2-binary`
* `pandas`, `python-dotenv`, `streamlit`, `plotly`, `sqlalchemy`

---

## Step 2: Fetch Data and Produce to Kafka (`fetch_raw.py`)

1. Enter `neo-app` container:

```bash
docker exec -it neo-app bash
```

2. Run fetch script:

```bash
python fetch_raw.py
# or python 1_fetch_and_produce.py
```

**Expected output:**

* Service readiness checks
* Fetch NEO data from NASA API
* Records published to Kafka (`neo-raw-data`)
* Data inserted into `neo_raw` table
* Success message

3. Verify database:

```bash
docker exec -it timescaledb psql -U neo_user -d neo_db
SELECT COUNT(*) FROM neo_raw;
SELECT * FROM neo_raw LIMIT 5;
\q
```

---

## Step 3: Verify Kafka Messages (`kafka_producer.py`)

1. Enter `neo-app`:

```bash
docker exec -it neo-app bash
```

2. Run verification:

```bash
python kafka_producer.py
# or python 2_kafka_consumer.py
```

**Expected output:**

* Kafka consumer reads messages from `neo-raw-data`
* Prints sample messages
* Success message for verified messages

**Troubleshooting:**
If no messages, rerun Step 2 or check Kafka logs:

```bash
docker logs kafka
```

---

## Step 4: Spark Processing (`spark_processor.py`)

1. Run Spark processing:

```bash
python spark_processor.py
# or python 3_spark_processor.py
```

**Expected output:**

* Reads from Kafka, computes risk scores, anomalies, mean diameters
* Writes to `neo_processed` table
* Stream runs; use `Ctrl+C` to stop
* Success message on stopping

2. Verify database:

```bash
docker exec -it timescaledb psql -U neo_user -d neo_db
SELECT COUNT(*) FROM neo_processed;
SELECT * FROM neo_processed LIMIT 5;
SELECT * FROM neo_processed WHERE is_anomaly = TRUE;
\q
```

**Tip:** Ensure Spark configuration includes required Kafka and Postgres drivers.

---

## Step 5: Calculate Daily Metrics (`calculate_metrics_data.py`)

1. Run metrics calculation:

```bash
python calculate_metrics_data.py
# or python 4_calculate_daily_metrics.py
```

**Expected output:**

* Daily metrics for each date range
* Success message

2. Verify database:

```bash
docker exec -it timescaledb psql -U neo_user -d neo_db
SELECT * FROM neo_daily_metrics ORDER BY metric_date;
\q
```

---

## Step 6: Streamlit UI (`streamlit_ui.py`)

1. Enter `neo-ui` container:

```bash
docker exec -it neo-ui bash
```

2. Run UI:

```bash
streamlit run streamlit_ui.py --server.port 8501 --server.address 0.0.0.0
# or streamlit run 5_streamlit_ui.py ...
```

3. Access dashboard:

* [http://localhost:8501](http://localhost:8501)
* Features:

  * Overview: total asteroids, hazardous count, avg risk, anomalies
  * Tabs: Raw Data, Processed Data, Daily Metrics, Top Risks
  * Interactions: filtering, CSV download, plots
* If no data, rerun previous steps

---

## Step 7: Grafana (Optional Visualization)

1. Access Grafana: [http://localhost:3000](http://localhost:3000), login `admin/admin`
2. Add PostgreSQL data source:

* Host: `timescaledb:5432`
* DB: `neo_db`
* User: `neo_user`
* Password: `neo_password`

3. Create dashboards querying `neo_processed` or `neo_daily_metrics`

**Example query:**

```sql
SELECT close_approach_date, risk_score FROM neo_processed
```

* Visualize as time-series charts

---

## Overall System Test

* Run Steps 2-6 sequentially for end-to-end verification
* Edge cases: Adjust `START_DATE` / `END_DATE` in `.env` and rerun
* Monitor logs for errors
* Reset system:

```bash
docker-compose down -v
```

---

**If any step fails**, collect container logs and error messages for debugging. This completes the end-to-end testing of the Neo Project.

```

---

If you want, I can also create a **shortened “Quick Copy-Paste Command Sequence” version** of this `.md` that lets you test everything in a few commands—perfect for rapid testing.  

Do you want me to do that?
```
