import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()


class Config:
    # --- Kafka Settings ---
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "neows_ingest")

    # --- Database Settings ---
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "neo_db")
    DB_USER = os.getenv("DB_USER", "neo_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "neo_password")

    # --- NASA API ---
    NASA_API_KEYS = [k.strip() for k in os.getenv("NASA_API_KEYS", "DEMO_KEY").split(",") if k.strip()]
    NASA_URL = os.getenv("NASA_URL", "https://api.nasa.gov/neo/rest/v1/feed")

    # --- App Logic ---
    START_DATE = os.getenv("START_DATE", "1899-12-30")
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "NeoWsStreamProcessor")
    PRODUCER_PARTITION_BY_DATE = os.getenv("PRODUCER_PARTITION_BY_DATE", "false").lower() == "true"

    # --- Paths ---
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))      # src/
    PROJECT_ROOT = os.path.dirname(BASE_DIR)                   # root/

    LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
    SPARK_CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints", "spark")
    PRODUCER_CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints", "producer")
    PIPELINE_STATUS_FILE = os.path.join(BASE_DIR, "checkpoints", "pipeline_status.json")

    # Run Metadata (set dynamically at runtime)
    RUN_ID = None