import json
import os
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, md5, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType

from src.config import Config
from src.logger import logger, console

AGENCY_TOPIC = "agency_ingest"
CHECKPOINT_DIR = "/tmp/spark-checkpoints/agency"

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


def none_if_blank(value):
    if value in ("", None):
        return None
    return value


def to_float(value):
    value = none_if_blank(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    value = none_if_blank(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def kafka_value_to_text(value):
    return value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # We get a raw JSON string from Kafka
    rows = batch_df.select("value").collect()
    
    # Process locally in Python since Spark JSON parsing for deeply nested 
    # highly-variable API structures is brittle and complex.
    # Since agency data volume is low (500 records/day), Python list processing is fast enough.
    
    sbdb_upserts, sbdb_history = [], []
    sentry_upserts, sentry_history = [], []
    esa_upserts, esa_history = [], []
    mpc_upserts, mpc_history = [], []
    cad_upserts, cad_history = [], []
    
    for row in rows:
        try:
            payload = json.loads(kafka_value_to_text(row["value"]))
            ast_id = payload.get("asteroid_id")
            agencies = payload.get("agencies", {})
            
            # --- 1. SBDB ---
            sbdb = agencies.get("sbdb")
            if sbdb and "object" in sbdb:
                obj = sbdb["object"]
                orb = sbdb.get("orbit", {})
                
                # Extract fields safely
                def get_elem(name):
                    for el in orb.get("elements", []):
                        if el.get("name") == name:
                            try:
                                return float(el.get("value"))
                            except:
                                return None
                    return None
                    
                sbdb_rec = {
                    "asteroid_id": ast_id,
                    "orbit_class": obj.get("orbit_class", {}).get("name"),
                    "spkid": obj.get("spkid"),
                    "epoch_tdb": to_float(orb.get("epoch")),
                    "data_arc_days": to_int(orb.get("data_arc")),
                    "n_obs_used": to_int(orb.get("n_obs_used")),
                    "condition_code": str(orb.get("condition_code")) if orb.get("condition_code") is not None else None,
                    "moid_au": to_float(orb.get("moid")),
                    "first_obs_date": none_if_blank(orb.get("first_obs")),
                    "last_obs_date": none_if_blank(orb.get("last_obs")),
                    "absolute_magnitude_h": get_elem("H"),
                    "diameter_km": None, # physical params usually separate
                    "albedo": None,
                    "discovery_date": none_if_blank(sbdb.get("discovery", {}).get("date")),
                    "discovery_site": sbdb.get("discovery", {}).get("site")
                }
                
                # Physical params
                for p in sbdb.get("phys_par", []):
                    if p["name"] == "diameter":
                        sbdb_rec["diameter_km"] = to_float(p.get("value"))
                    if p["name"] == "albedo":
                        sbdb_rec["albedo"] = to_float(p.get("value"))
                
                # Hash for history
                hash_str = str(sbdb_rec["epoch_tdb"]) + str(sbdb_rec["n_obs_used"])
                sbdb_rec["row_hash"] = hash_str
                sbdb_upserts.append(sbdb_rec)
                
            # --- 2. SENTRY ---
            sentry = agencies.get("sentry")
            if sentry:
                summary = sentry.get("summary", {})
                status = "removed" if "removed" in sentry else "active"
                
                sentry_rec = {
                    "asteroid_id": ast_id,
                    "status": status,
                    "impact_probability": to_float(summary.get("ip")),
                    "torino_scale": to_int(summary.get("ts_max")) or 0,
                    "palermo_scale": to_float(summary.get("ps_cum")),
                    "n_impacts": to_int(summary.get("n_imp")) or 0,
                    "v_infinity_km_s": to_float(summary.get("v_inf")),
                    "energy_mt": to_float(summary.get("energy")),
                }
                sentry_rec["row_hash"] = str(sentry_rec["impact_probability"]) + str(sentry_rec["n_impacts"])
                sentry_upserts.append(sentry_rec)
                
            # --- 3. ESA ---
            esa = agencies.get("esa")
            if esa:
                esa_rec = {
                    "asteroid_id": ast_id,
                    "on_risk_list": esa.get("on_risk_list", False),
                    "priority_list": 0 # Not parsed easily from text, defaulting
                }
                esa_rec["row_hash"] = str(esa_rec["on_risk_list"])
                esa_upserts.append(esa_rec)
                
            # --- 4. MPC ---
            mpc = agencies.get("mpc")
            if mpc:
                mpc_first = mpc[0] if isinstance(mpc, list) and mpc else mpc
                orb = mpc_first.get("mpc_orb", mpc_first if isinstance(mpc_first, dict) else {})
                mpc_rec = {
                    "asteroid_id": ast_id,
                    "status": "found",
                    "mpc_designation": orb.get("number") or orb.get("designation"),
                    "epoch": to_float(orb.get("epoch")),
                    "eccentricity": to_float(orb.get("eccentricity")),
                    "inclination": to_float(orb.get("inclination")),
                }
                mpc_rec["row_hash"] = str(mpc_rec["epoch"])
                mpc_upserts.append(mpc_rec)
                
            # --- 5. CAD ---
            cad = agencies.get("cad")
            if cad and "data" in cad:
                fields = cad.get("fields", [])
                for cad_row in cad["data"][:50]:
                    entry = dict(zip(fields, cad_row))
                    try:
                        cad_rec = {
                            "asteroid_id": ast_id,
                            "approach_date": entry.get("cd").split(" ")[0] if entry.get("cd") else None,
                            "distance_au": to_float(entry.get("dist")),
                            "v_rel_km_s": to_float(entry.get("v_rel"))
                        }
                        if cad_rec["approach_date"]:
                            cad_rec["row_hash"] = str(cad_rec["distance_au"])
                            cad_upserts.append(cad_rec)
                    except:
                        pass
                        
        except Exception as e:
            logger.error(f"Error parsing JSON payload: {e}")

    # ============================================
    # DB WRITES
    # ============================================
    with db_connection() as conn:
        with conn.cursor() as cur:
            
            def upsert_and_history(table, pk_cols, records):
                if not records: return
                cols = list(records[0].keys())
                
                # Fetch existing to compare hashes
                ast_ids = list(set([r["asteroid_id"] for r in records]))
                if not ast_ids: return
                
                cur.execute(f"SELECT {','.join(pk_cols)}, row_hash FROM {table} WHERE asteroid_id = ANY(%s)", (ast_ids,))
                existing = cur.fetchall()
                existing_dict = {tuple(str(r[i]) for i in range(len(pk_cols))): r[-1] for r in existing}
                
                inserts = []
                updates = []
                histories = []
                
                for rec in records:
                    pk_val = tuple(str(rec[c]) for c in pk_cols)
                    old_hash = existing_dict.get(pk_val)
                    
                    if old_hash is None:
                        inserts.append(rec)
                        hist = dict(rec)
                        hist["change_type"] = "INSERT"
                        histories.append(hist)
                    elif old_hash != rec.get("row_hash"):
                        updates.append(rec)
                        hist = dict(rec)
                        hist["change_type"] = "UPDATE"
                        histories.append(hist)
                        
                to_upsert = inserts + updates
                if to_upsert:
                    vals = [[r[c] for c in cols] for r in to_upsert]
                    updates_sql = [f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_cols]
                    sql = f"""
                        INSERT INTO {table} ({','.join(cols)}) VALUES %s
                        ON CONFLICT ({','.join(pk_cols)})
                        DO UPDATE SET {','.join(updates_sql)}
                    """
                    chunked_execute_values(cur, sql, vals)
                    
                if histories:
                    h_cols = cols + ["change_type"]
                    h_vals = [[r.get(c) for c in h_cols] for r in histories]
                    h_sql = f"INSERT INTO {table}_history ({','.join(h_cols)}) VALUES %s"
                    chunked_execute_values(cur, h_sql, h_vals)
                    
            upsert_and_history("neo_agency_sbdb", ["asteroid_id"], sbdb_upserts)
            upsert_and_history("neo_agency_sentry", ["asteroid_id"], sentry_upserts)
            upsert_and_history("neo_agency_esa", ["asteroid_id"], esa_upserts)
            upsert_and_history("neo_agency_mpc", ["asteroid_id"], mpc_upserts)
            upsert_and_history("neo_agency_cad", ["asteroid_id", "approach_date"], cad_upserts)

        conn.commit()
    
    logger.info(f"✅ Processed Agency Batch {batch_id} - [{len(sbdb_upserts)} SBDB, {len(sentry_upserts)} Sentry]")


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
    logger.info("🚀 Starting Agency Spark Processor...")
    
    # Ensure topic exists
    ensure_topic(AGENCY_TOPIC)

    spark = SparkSession.builder \
        .appName("AgencyProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.port", "4041") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", AGENCY_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
        
    query = raw_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
