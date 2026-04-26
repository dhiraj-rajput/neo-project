import json
import os
import time
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, md5, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, ArrayType

from src.config import Config
from src.logger import logger, console
from rich.table import Table
from rich.panel import Panel

# Topics
NEOWS_TOPIC = "neows_ingest"
AGENCY_TOPIC = "agency_ingest"

# Checkpoints
NEOWS_CHECKPOINT = os.path.join(Config.SPARK_CHECKPOINT_DIR, "neows")
AGENCY_CHECKPOINT = os.path.join(Config.SPARK_CHECKPOINT_DIR, "agency")

@contextmanager
def db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST, port=Config.DB_PORT, 
            dbname=Config.DB_NAME, user=Config.DB_USER, password=Config.DB_PASSWORD
        )
        yield conn
    except Exception as e:
        logger.error(f"DB Connection Error: {e}")
        raise e
    finally:
        if conn:
            conn.close()

def chunked_execute_values(cur, sql, vals, chunk_size=1000):
    for i in range(0, len(vals), chunk_size):
        execute_values(cur, sql, vals[i:i+chunk_size])

# =========================================================
# BATCH PROCESSORS
# =========================================================

def process_neows_batch(batch_df, batch_id):
    """Processes basic close-approach data from NASA NeoWs."""
    if batch_df.isEmpty(): return

    # Deduplicate
    batch_df = batch_df.dropna(subset=['close_approach_date', 'asteroid_id']) \
                       .dropDuplicates(['close_approach_date', 'asteroid_id'])
    
    # Date formatting
    batch_df = batch_df.withColumn("close_approach_date", to_date(col("close_approach_date"), "yyyy-MM-dd"))
    
    # Hashing
    exclude_cols = ['ingestion_time', 'row_hash']
    hash_cols = sorted([c for c in batch_df.columns if c not in exclude_cols])
    batch_hashed = batch_df.withColumn("row_hash", md5(concat_ws("||", *[col(c) for c in hash_cols])))
    
    rows = batch_hashed.collect()
    if not rows: return

    with db_connection() as conn:
        with conn.cursor() as cur:
            # 1. Fetch existing hashes
            ast_ids = list(set([r['asteroid_id'] for r in rows]))
            in_clause = ",".join([f"'{aid}'" for aid in ast_ids])
            cur.execute(f"SELECT close_approach_date, asteroid_id, row_hash FROM neo_close_approaches WHERE asteroid_id IN ({in_clause})")
            existing = cur.fetchall()
            existing_hashes = {(str(r[0]), r[1]): r[2] for r in existing}

            to_upsert = []
            to_history = []

            for r in rows:
                key = (str(r['close_approach_date']), r['asteroid_id'])
                old_hash = existing_hashes.get(key)
                
                if old_hash is None or old_hash != r['row_hash']:
                    to_upsert.append(r)
                    to_history.append({**r.asDict(), "change_type": "INSERT" if old_hash is None else "UPDATE"})

            # 2. Write History
            if to_history:
                cols = list(to_history[0].keys())
                vals = [[r[c] for c in cols] for r in to_history]
                sql = f"INSERT INTO neo_close_approaches_history ({','.join(cols)}) VALUES %s"
                chunked_execute_values(cur, sql, vals)

            # 3. Write Main
            if to_upsert:
                cols = batch_hashed.columns
                vals = [[r[c] for c in cols] for r in to_upsert]
                updates_sql = [f"{c} = EXCLUDED.{c}" for c in cols if c not in ["close_approach_date", "asteroid_id"]]
                sql = f"""
                    INSERT INTO neo_close_approaches ({','.join(cols)}) VALUES %s
                    ON CONFLICT (close_approach_date, asteroid_id)
                    DO UPDATE SET {','.join(updates_sql)}
                """
                chunked_execute_values(cur, sql, vals)

        conn.commit()
    logger.info(f"✅ NeoWs Batch {batch_id}: Processed {len(to_upsert)} records.")

def process_agency_batch(batch_df, batch_id):
    """Processes deep-profile data from 5 agencies (SBDB, Sentry, MPC, etc)."""
    if batch_df.isEmpty(): return

    rows = batch_df.select("value").collect()
    
    sbdb_recs, sentry_recs, esa_recs, mpc_recs, cad_recs = [], [], [], [], []
    
    for row in rows:
        try:
            p = json.loads(row['value'].decode('utf-8'))
            ast_id = p.get("asteroid_id")
            ag = p.get("agencies", {})
            
            # SBDB
            if ag.get("sbdb"):
                s = ag["sbdb"]
                sbdb_recs.append({
                    "asteroid_id": ast_id, "orbit_class": s.get("object", {}).get("orbit_class", {}).get("name"),
                    "spkid": s.get("object", {}).get("spkid"), "row_hash": str(s.get("orbit", {}).get("epoch"))
                })
            # Sentry
            if ag.get("sentry") and "summary" in ag["sentry"]:
                s = ag["sentry"]["summary"]
                sentry_recs.append({
                    "asteroid_id": ast_id, "status": "active", "impact_probability": float(s.get("ip") or 0),
                    "torino_scale": int(s.get("ts_max") or 0), "palermo_scale": float(s.get("ps_cum") or 0),
                    "row_hash": str(s.get("ip"))
                })
            # ESA
            if ag.get("esa"):
                esa_recs.append({
                    "asteroid_id": ast_id, "on_risk_list": ag["esa"].get("on_risk_list", False), "row_hash": str(ag["esa"].get("on_risk_list"))
                })
            # MPC
            if ag.get("mpc") and len(ag["mpc"]) > 0:
                m = ag["mpc"][0].get("mpc_orb", {})
                mpc_recs.append({
                    "asteroid_id": ast_id, "mpc_designation": m.get("designation"), "row_hash": str(m.get("epoch"))
                })
            # CAD
            if ag.get("cad") and "data" in ag["cad"]:
                fields = ag["cad"]["fields"]
                for r in ag["cad"]["data"][:10]:
                    entry = dict(zip(fields, r))
                    cad_recs.append({
                        "asteroid_id": ast_id, "approach_date": entry.get("cd").split(" ")[0],
                        "distance_au": float(entry.get("dist")), "v_rel_km_s": float(entry.get("v_rel")),
                        "row_hash": str(entry.get("dist"))
                    })
        except: continue

    with db_connection() as conn:
        with conn.cursor() as cur:
            def upsert(table, pk, records):
                if not records: return
                cols = list(records[0].keys())
                vals = [[r[c] for c in cols] for r in records]
                up_sql = [f"{c} = EXCLUDED.{c}" for c in cols if c not in pk]
                sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES %s ON CONFLICT ({','.join(pk)}) DO UPDATE SET {','.join(up_sql)}"
                chunked_execute_values(cur, sql, vals)
                # History (simplified)
                h_cols = cols + ["change_type"]
                h_vals = [[r.get(c) for c in cols] + ["SYNC"] for r in records]
                cur.execute(f"INSERT INTO {table}_history ({','.join(h_cols)}) VALUES %s", (h_vals[0],)) # sample for one

            upsert("neo_agency_sbdb", ["asteroid_id"], sbdb_recs)
            upsert("neo_agency_sentry", ["asteroid_id"], sentry_recs)
            upsert("neo_agency_esa", ["asteroid_id"], esa_recs)
            upsert("neo_agency_mpc", ["asteroid_id"], mpc_recs)
            upsert("neo_agency_cad", ["asteroid_id", "approach_date"], cad_recs)
        conn.commit()
    logger.info(f"✅ Agency Batch {batch_id}: Multi-Agency data synced.")

# =========================================================
# SCHEMAS
# =========================================================

def get_neows_schema():
    dia_unit = StructType([StructField("estimated_diameter_min", DoubleType()), StructField("estimated_diameter_max", DoubleType())])
    vel = StructType([StructField("kilometers_per_second", StringType()), StructField("kilometers_per_hour", StringType()), StructField("miles_per_hour", StringType())])
    dist = StructType([StructField("astronomical", StringType()), StructField("lunar", StringType()), StructField("kilometers", StringType()), StructField("miles", StringType())])
    cad_schema = StructType([StructField("close_approach_date_full", StringType()), StructField("epoch_date_close_approach", DoubleType()), StructField("orbiting_body", StringType()), StructField("relative_velocity", vel), StructField("miss_distance", dist)])
    ast_schema = StructType([
        StructField("id", StringType()), StructField("neo_reference_id", StringType()), StructField("name", StringType()), 
        StructField("absolute_magnitude_h", DoubleType()), StructField("is_potentially_hazardous_asteroid", BooleanType()), 
        StructField("is_sentry_object", BooleanType()), StructField("nasa_jpl_url", StringType()), 
        StructField("estimated_diameter", StructType([StructField("kilometers", dia_unit), StructField("meters", dia_unit), StructField("miles", dia_unit), StructField("feet", dia_unit)])), 
        StructField("close_approach_data", ArrayType(cad_schema))
    ])
    return StructType([StructField("date", StringType()), StructField("asteroid", ast_schema)])

def flatten_neows(df):
    df = df.select(col("date"), col("asteroid.*"))
    df = df.withColumn("cad", col("close_approach_data")[0])
    return df.select(
        col("date").alias("close_approach_date"), col("id").alias("asteroid_id"), col("name"),
        col("absolute_magnitude_h"), col("is_potentially_hazardous_asteroid").alias("is_potentially_hazardous"),
        col("estimated_diameter.kilometers.estimated_diameter_max").alias("estimated_diameter_km_max"),
        col("cad.relative_velocity.kilometers_per_second").alias("relative_velocity_km_s"),
        col("cad.miss_distance.kilometers").alias("miss_distance_km"),
        col("nasa_jpl_url")
    )

def ensure_topic(topic_name):
    """Ensures a Kafka topic exists before Spark attempts to read from it."""
    from kafka.admin import KafkaAdminClient, NewTopic
    try:
        admin = KafkaAdminClient(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
        if topic_name not in admin.list_topics():
            logger.info(f"✨ Creating missing topic: {topic_name}")
            admin.create_topics([NewTopic(name=topic_name, num_partitions=6, replication_factor=1)])
        admin.close()
    except Exception as e:
        logger.warning(f"⚠️ Topic verification failed for {topic_name}: {e}")

def main():
    logger.info("🚀 Starting NeoWs Spark Processor...")
    
    # 0. Ensure topic exists
    ensure_topic(NEOWS_TOPIC)

    spark = SparkSession.builder \
        .appName("NeoWs-Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.port", "4040") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(NEOWS_CHECKPOINT, exist_ok=True)

    # --- NeoWs Stream ---
    neows_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", NEOWS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    
    neows_flat = flatten_neows(neows_raw.select(from_json(col("value").cast("string"), get_neows_schema()).alias("d")).select("d.*"))
    
    query = neows_flat.writeStream \
        .foreachBatch(process_neows_batch) \
        .option("checkpointLocation", NEOWS_CHECKPOINT) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info(f"📡 NeoWs Stream Active on topic: {NEOWS_TOPIC}")
    query.awaitTermination()

if __name__ == "__main__":
    main()