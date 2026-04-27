"""
NeoWs Spark Structured Streaming Processor

Consumes raw NeoWs close-approach data from the 'neows_ingest' Kafka topic,
flattens the nested JSON, computes row hashes for CDC, and upserts into
the neo_close_approaches TimescaleDB hypertable.

NOTE: Agency data processing is handled entirely by agency_processor.py.
      This file is NeoWs-only — no duplicate agency code.
"""

import json
import os
import time

import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, concat_ws, md5, lit, to_date, explode, size,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, ArrayType,
)

from src.config import Config
from src.logger import logger, console

# ── Configuration ────────────────────────────────────────────

NEOWS_TOPIC = "neows_ingest"
NEOWS_CHECKPOINT = os.path.join(Config.SPARK_CHECKPOINT_DIR, "neows")


# ── Database Helpers ─────────────────────────────────────────

@contextmanager
def db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST, port=Config.DB_PORT,
            dbname=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD,
        )
        yield conn
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def chunked_execute_values(cur, sql, vals, chunk_size=1000):
    """Batch insert using psycopg2's fast execute_values, in chunks."""
    for i in range(0, len(vals), chunk_size):
        execute_values(cur, sql, vals[i : i + chunk_size])


# ── NeoWs Schema & Flattening ────────────────────────────────

def get_neows_schema():
    """Build the Spark schema for NeoWs JSON messages."""
    dia_unit = StructType([
        StructField("estimated_diameter_min", DoubleType()),
        StructField("estimated_diameter_max", DoubleType()),
    ])
    vel = StructType([
        StructField("kilometers_per_second", StringType()),
        StructField("kilometers_per_hour", StringType()),
        StructField("miles_per_hour", StringType()),
    ])
    dist = StructType([
        StructField("astronomical", StringType()),
        StructField("lunar", StringType()),
        StructField("kilometers", StringType()),
        StructField("miles", StringType()),
    ])
    cad_schema = StructType([
        StructField("close_approach_date_full", StringType()),
        StructField("epoch_date_close_approach", DoubleType()),
        StructField("orbiting_body", StringType()),
        StructField("relative_velocity", vel),
        StructField("miss_distance", dist),
    ])
    ast_schema = StructType([
        StructField("id", StringType()),
        StructField("neo_reference_id", StringType()),
        StructField("name", StringType()),
        StructField("absolute_magnitude_h", DoubleType()),
        StructField("is_potentially_hazardous_asteroid", BooleanType()),
        StructField("is_sentry_object", BooleanType()),
        StructField("nasa_jpl_url", StringType()),
        StructField("estimated_diameter", StructType([
            StructField("kilometers", dia_unit),
            StructField("meters", dia_unit),
            StructField("miles", dia_unit),
            StructField("feet", dia_unit),
        ])),
        StructField("close_approach_data", ArrayType(cad_schema)),
    ])
    return StructType([
        StructField("date", StringType()),
        StructField("asteroid", ast_schema),
    ])


def flatten_neows(df):
    """
    Flatten the nested NeoWs JSON into a flat row per close-approach event.

    FIX: Now uses explode() to handle multiple close-approach records per
    asteroid per date, instead of only taking [0].
    """
    # Unpack asteroid-level fields
    df = df.select(col("date"), col("asteroid.*"))

    # Explode close_approach_data array → one row per approach
    df = df.withColumn("cad", explode(col("close_approach_data")))

    return df.select(
        col("date").alias("close_approach_date"),
        col("id").alias("asteroid_id"),
        col("neo_reference_id"),
        col("name"),
        col("absolute_magnitude_h"),
        col("is_potentially_hazardous_asteroid").alias("is_potentially_hazardous"),
        col("is_sentry_object"),

        # Diameter (all units)
        col("estimated_diameter.kilometers.estimated_diameter_min").alias("estimated_diameter_km_min"),
        col("estimated_diameter.kilometers.estimated_diameter_max").alias("estimated_diameter_km_max"),
        col("estimated_diameter.meters.estimated_diameter_min").alias("estimated_diameter_m_min"),
        col("estimated_diameter.meters.estimated_diameter_max").alias("estimated_diameter_m_max"),
        col("estimated_diameter.miles.estimated_diameter_min").alias("estimated_diameter_miles_min"),
        col("estimated_diameter.miles.estimated_diameter_max").alias("estimated_diameter_miles_max"),
        col("estimated_diameter.feet.estimated_diameter_min").alias("estimated_diameter_feet_min"),
        col("estimated_diameter.feet.estimated_diameter_max").alias("estimated_diameter_feet_max"),

        # Close-approach data
        col("cad.close_approach_date_full"),
        col("cad.epoch_date_close_approach"),
        col("cad.orbiting_body"),

        # Velocity
        col("cad.relative_velocity.kilometers_per_second").alias("relative_velocity_km_s"),
        col("cad.relative_velocity.kilometers_per_hour").alias("relative_velocity_km_h"),
        col("cad.relative_velocity.miles_per_hour").alias("relative_velocity_mph"),

        # Miss distance
        col("cad.miss_distance.astronomical").alias("miss_distance_astronomical"),
        col("cad.miss_distance.lunar").alias("miss_distance_lunar"),
        col("cad.miss_distance.kilometers").alias("miss_distance_km"),
        col("cad.miss_distance.miles").alias("miss_distance_miles"),

        col("nasa_jpl_url"),
    )


# ── Batch Processor ──────────────────────────────────────────

def process_neows_batch(batch_df, batch_id):
    """Processes NeoWs close-approach data: dedup, hash, upsert + history."""
    if batch_df.rdd.isEmpty():
        return

    # Deduplicate
    batch_df = batch_df.dropna(subset=["close_approach_date", "asteroid_id"]) \
                       .dropDuplicates(["close_approach_date", "asteroid_id"])

    # Date formatting
    batch_df = batch_df.withColumn("close_approach_date", to_date(col("close_approach_date"), "yyyy-MM-dd"))

    # Hashing (all columns except meta)
    exclude_cols = {"ingestion_time", "row_hash"}
    hash_cols = sorted([c for c in batch_df.columns if c not in exclude_cols])
    batch_hashed = batch_df.withColumn("row_hash", md5(concat_ws("||", *[col(c) for c in hash_cols])))

    rows = batch_hashed.collect()
    if not rows:
        return

    with db_connection() as conn:
        with conn.cursor() as cur:
            # 1. Fetch existing hashes for CDC
            ast_ids = list({r["asteroid_id"] for r in rows})
            # Use parameterised query to avoid SQL injection
            cur.execute(
                "SELECT close_approach_date, asteroid_id, row_hash "
                "FROM neo_close_approaches WHERE asteroid_id = ANY(%s)",
                (ast_ids,),
            )
            existing = cur.fetchall()
            existing_hashes = {(str(r[0]), r[1]): r[2] for r in existing}

            to_upsert = []
            to_history = []

            for r in rows:
                key = (str(r["close_approach_date"]), r["asteroid_id"])
                old_hash = existing_hashes.get(key)

                if old_hash is None or old_hash != r["row_hash"]:
                    to_upsert.append(r)
                    to_history.append({
                        **r.asDict(),
                        "change_type": "INSERT" if old_hash is None else "UPDATE",
                    })

            # 2. Write history
            if to_history:
                cols = list(to_history[0].keys())
                vals = [[r[c] for c in cols] for r in to_history]
                sql = f"INSERT INTO neo_close_approaches_history ({','.join(cols)}) VALUES %s"
                chunked_execute_values(cur, sql, vals)

            # 3. Upsert main table
            if to_upsert:
                cols = batch_hashed.columns
                vals = [[r[c] for c in cols] for r in to_upsert]
                updates_sql = [
                    f"{c} = EXCLUDED.{c}"
                    for c in cols
                    if c not in ("close_approach_date", "asteroid_id")
                ]
                sql = f"""
                    INSERT INTO neo_close_approaches ({','.join(cols)}) VALUES %s
                    ON CONFLICT (close_approach_date, asteroid_id)
                    DO UPDATE SET {','.join(updates_sql)}
                """
                chunked_execute_values(cur, sql, vals)

        conn.commit()

    logger.info(f"NeoWs batch {batch_id}: {len(to_upsert)} upserts, {len(to_history)} history records.")


# ── Topic Safety ─────────────────────────────────────────────

def ensure_topic(topic_name):
    """Create a Kafka topic if it doesn't already exist."""
    from kafka.admin import KafkaAdminClient, NewTopic
    try:
        admin = KafkaAdminClient(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
        if topic_name not in admin.list_topics():
            logger.info(f"Creating missing Kafka topic: {topic_name}")
            admin.create_topics([NewTopic(name=topic_name, num_partitions=6, replication_factor=1)])
        admin.close()
    except Exception as e:
        logger.warning(f"Topic verification failed for {topic_name}: {e}")


# ── Main Entry Point ────────────────────────────────────────

def main():
    console.rule("[pipeline]NeoWs Spark Processor")
    logger.info("Starting NeoWs Spark processor…")

    ensure_topic(NEOWS_TOPIC)

    spark = SparkSession.builder \
        .appName("NeoWs-Processor") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.port", "4040") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(NEOWS_CHECKPOINT, exist_ok=True)

    # NeoWs stream
    neows_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", NEOWS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load()

    neows_parsed = neows_raw.select(
        from_json(col("value").cast("string"), get_neows_schema()).alias("d")
    ).select("d.*")

    neows_flat = flatten_neows(neows_parsed)

    query = neows_flat.writeStream \
        .foreachBatch(process_neows_batch) \
        .option("checkpointLocation", NEOWS_CHECKPOINT) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info(f"NeoWs stream active on topic: {NEOWS_TOPIC}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
