import json
import os
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, md5, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, ArrayType
from rich.table import Table
from rich.panel import Panel

from src.config import Config
from src.logger import logger, console

# =========================================================
# DB CONTEXT MANAGER (Prevents Leaks)
# =========================================================
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

# =========================================================
# PROCESS BATCH
# =========================================================
def chunked_execute_values(cur, sql, vals, chunk_size=1000):
    """Helper to write large lists in chunks to avoid query length limits."""
    for i in range(0, len(vals), chunk_size):
        execute_values(cur, sql, vals[i:i+chunk_size])

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"⚠️ Batch {batch_id}: Skipped (Empty after filtering). Check Producer/Schema match.")
        return

    # 1. Deduplicate and REMOVE NULL KEYS
    batch_df = batch_df.dropna(subset=['close_approach_date', 'asteroid_id']) \
                       .dropDuplicates(['close_approach_date', 'asteroid_id'])
    
    if batch_df.isEmpty():
        logger.info(f"\n\n ⚡ Batch {batch_id}: No valid records after filtering nulls.\n\n")
        return

    count = batch_df.count()
    console.print(Panel(
        f"Processing [bold cyan]{count:,}[/bold cyan] unique records",
        title=f"⚡ Batch {batch_id}",
        border_style="cyan",
    ))
    
    spark = SparkSession.getActiveSession()
    
    # 2. Type Safety & Optimization
    # Convert string date to DateType for correct DB comparison
    from pyspark.sql.functions import to_date
    batch_df = batch_df.withColumn("close_approach_date", to_date(col("close_approach_date"), "yyyy-MM-dd"))

    # 3. Safe DB Lookup (Option A: Date-Batched)
    # Instead of pulling ALL keys (tuple IN clause), we pull only distinct DATES.
    # This reduces driver memory usage and query size drastically.
    distinct_dates = [row['close_approach_date'] for row in batch_df.select("close_approach_date").distinct().collect()]
    
    jdbc_url = f"jdbc:postgresql://{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
    existing_df = None
    
    # Chunk dates to avoid huge IN clauses even for dates
    date_chunks = [distinct_dates[i:i+50] for i in range(0, len(distinct_dates), 50)]
    
    try:
        if not date_chunks:
            # Should not happen as we checked isEmpty, but handling safety
            existing_df = spark.createDataFrame([], batch_df.schema) # Empty placeholder with rough schema
        else:
            for chunk in date_chunks:
                # Format dates for SQL IN clause
                safe_dates = ",".join(f"'{d.strftime('%Y-%m-%d')}'" for d in chunk)
                query = f"(SELECT * FROM neo_close_approaches WHERE close_approach_date IN ({safe_dates})) as tmp"
                
                df_chunk = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", query) \
                    .option("user", Config.DB_USER) \
                    .option("password", Config.DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()
                
                if existing_df is None:
                    existing_df = df_chunk
                else:
                    existing_df = existing_df.union(df_chunk)

        # 4. Compute Hash & Diff
        exclude_cols = ['ingestion_time', 'row_hash']
        # Ensure we only hash columns present in batch
        hash_cols = sorted([c for c in batch_df.columns if c not in exclude_cols])

        batch_hashed = batch_df.withColumn("row_hash", md5(concat_ws("||", *[col(c) for c in hash_cols])))
        
        # Rename existing columns for join
        # Only alias columns that exist in DB result
        existing_cols = existing_df.columns
        existing_renamed = existing_df.select([col(c).alias(f"old_{c}") for c in existing_cols])

        joined = batch_hashed.join(
            existing_renamed, 
            (col("close_approach_date") == col("old_close_approach_date")) & 
            (col("asteroid_id") == col("old_asteroid_id")),
            "left"
        )

        changed_df = joined.filter(col("old_row_hash").isNull() | (col("row_hash") != col("old_row_hash")))
        
        if changed_df.isEmpty():
            logger.info(f"✅ Batch {batch_id}: No changes (Data matches DB).\n")
            return

        changed_df.cache()

        # 5. Prepare Writes
        # Main Table (New State)
        delta_main = changed_df.select(*batch_df.columns, "row_hash")
        
        # History Table (Archive Old State)
        inserts = changed_df.filter(col("old_row_hash").isNull()) \
            .select(*batch_df.columns, "row_hash").withColumn("change_type", lit("INSERT"))
            
        # Map old columns back to original names for history
        # We need to map only valid columns from batch that existed in old
        common_cols = [c for c in batch_df.columns if f"old_{c}" in existing_renamed.columns]
        old_cols_select = [col(f"old_{c}").alias(c) for c in common_cols]
        
        updates = changed_df.filter(col("old_row_hash").isNotNull()) \
            .select(*old_cols_select, col("old_row_hash").alias("row_hash")) \
            .withColumn("change_type", lit("UPDATE"))
            
        history_df = inserts.union(updates)

        # Ordering
        delta_main = delta_main.orderBy(col("close_approach_date").asc(), col("asteroid_id").asc())
        history_df = history_df.orderBy(col("close_approach_date").asc(), col("asteroid_id").asc())

        n_inserts = inserts.count()
        n_updates = updates.count()

        batch_table = Table(title=f"📊 Batch {batch_id} Stats", show_header=True, header_style="bold cyan")
        batch_table.add_column("Operation", style="dim")
        batch_table.add_column("Count", justify="right", style="bold")
        batch_table.add_row("🆕 New (INSERT)", f"[green]{n_inserts:,}[/green]")
        batch_table.add_row("🔄 Changed (UPDATE)", f"[yellow]{n_updates:,}[/yellow]")
        console.print(batch_table)

        # 6. Write to DB with Batched Execution
        with db_connection() as conn:
            with conn.cursor() as cur:
                # Write History
                hist_rows = history_df.collect()
                if hist_rows:
                    cols = history_df.columns
                    vals = [[r[c] for c in cols] for r in hist_rows]
                    sql = f"INSERT INTO neo_close_approaches_history ({','.join(cols)}) VALUES %s"
                    chunked_execute_values(cur, sql, vals, chunk_size=1000)
                    logger.info(f"   📜 Archived {len(hist_rows)} records to History table.")
                
                # Write Main (Upsert)
                main_rows = delta_main.collect()
                if main_rows:
                    cols = delta_main.columns
                    vals = [[r[c] for c in cols] for r in main_rows]
                    
                    key_cols = ["close_approach_date", "asteroid_id"]
                    # Safe update set construction
                    updates_sql = [f"{c} = EXCLUDED.{c}" for c in cols if c not in key_cols]
                    
                    sql = f"""
                        INSERT INTO neo_close_approaches ({','.join(cols)}) VALUES %s
                        ON CONFLICT ({','.join(key_cols)})
                        DO UPDATE SET {','.join(updates_sql)}
                    """
                    chunked_execute_values(cur, sql, vals, chunk_size=1000)
                    logger.info(f"   💾 Upserted {len(main_rows)} records to Main table.")

                conn.commit()
                logger.info(f"✅ Batch {batch_id} DB Sync Complete.\n")

        changed_df.unpersist()

    except Exception as e:
        logger.error(f"🔥 Batch {batch_id} Failed: {e}")
        raise e

# =========================================================
# SCHEMA & FLATTEN (Kept concise)
# =========================================================
def get_schema():
    # Ensure this matches exactly what you had.
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

def apply_flattening(df):
    df = df.select(col("date"), col("asteroid.*"))
    df = df.withColumn("cad", col("close_approach_data")[0])
    # ... (Rest of your flattening logic) ...
    # Use exact same projection as your previous file
    return df.select(
        col("date").alias("close_approach_date"), col("id").alias("asteroid_id"), col("neo_reference_id"), col("name"),
        col("absolute_magnitude_h"), col("is_potentially_hazardous_asteroid").alias("is_potentially_hazardous"),
        col("is_sentry_object"), col("nasa_jpl_url"),
        col("estimated_diameter.kilometers.estimated_diameter_min").alias("estimated_diameter_km_min"),
        col("estimated_diameter.kilometers.estimated_diameter_max").alias("estimated_diameter_km_max"),
        col("estimated_diameter.meters.estimated_diameter_min").alias("estimated_diameter_m_min"),
        col("estimated_diameter.meters.estimated_diameter_max").alias("estimated_diameter_m_max"),
        col("estimated_diameter.miles.estimated_diameter_min").alias("estimated_diameter_miles_min"),
        col("estimated_diameter.miles.estimated_diameter_max").alias("estimated_diameter_miles_max"),
        col("estimated_diameter.feet.estimated_diameter_min").alias("estimated_diameter_feet_min"),
        col("estimated_diameter.feet.estimated_diameter_max").alias("estimated_diameter_feet_max"),
        col("cad.close_approach_date_full"), col("cad.epoch_date_close_approach"), col("cad.orbiting_body"),
        col("cad.relative_velocity.kilometers_per_second").alias("relative_velocity_km_s"),
        col("cad.relative_velocity.kilometers_per_hour").alias("relative_velocity_km_h"),
        col("cad.relative_velocity.miles_per_hour").alias("relative_velocity_mph"),
        col("cad.miss_distance.astronomical").alias("miss_distance_astronomical"),
        col("cad.miss_distance.lunar").alias("miss_distance_lunar"),
        col("cad.miss_distance.kilometers").alias("miss_distance_km"),
        col("cad.miss_distance.miles").alias("miss_distance_miles")
    )

# =========================================================
# MAIN
# =========================================================
import time  # noqa: E402 — placed here to avoid circular import with logger

def main():
    logger.info("🚀 Starting Spark Stream Processor...")
    
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    while True:
        logger.info("🔄 Starting 5-minute idle monitoring cycle...")

        try:
            # Read from Kafka (Removed read_committed to ensure compatibility)
            raw_df = spark.readStream.format("kafka") \
                .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", Config.KAFKA_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("maxOffsetsPerTrigger", 1000) \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Robust Parsing
            json_df = raw_df.select(col("value").cast("string").alias("json_str"))
            
            parsed = json_df.select(from_json(col("json_str"), get_schema(), options={"mode": "PERMISSIVE"}).alias("data")) \
                            .select("data.*")
            
            # Critical: Filter out rows where parsing failed
            valid_df = parsed.filter(col("date").isNotNull() & col("asteroid.id").isNotNull())
            
            flat_df = apply_flattening(valid_df)

            # Clean up checkpoints
            os.makedirs(Config.SPARK_CHECKPOINT_DIR, exist_ok=True)

            query = flat_df.writeStream \
                .foreachBatch(process_batch) \
                .option("checkpointLocation", Config.SPARK_CHECKPOINT_DIR) \
                .trigger(processingTime="10 seconds") \
                .start()

            # --- IDLE MONITORING LOOP ---
            last_data_time = time.time()
            last_batch_id = -1
            last_heartbeat = time.time()
            
            def check_pipeline_status():
                """Checks the shared status file to see if Producer is alive/sleeping."""
                if not os.path.exists(Config.PIPELINE_STATUS_FILE):
                    return None
                    
                try:
                    with open(Config.PIPELINE_STATUS_FILE, 'r') as f:
                        return json.load(f)
                except Exception:
                    return None

            while query.isActive:
                time.sleep(5) # Check every 5s
                
                # Check query progress
                if query.lastProgress:
                    curr_id = query.lastProgress['batchId']
                    # If this is a new batch
                    if curr_id != last_batch_id:
                        last_batch_id = curr_id
                        # Check if it actually had data (processedRowsPerSecond or numInputRows)
                        if query.lastProgress['numInputRows'] > 0:
                            last_data_time = time.time()
                            logger.info(f"⚡ Batch {curr_id} processed rows. Timer reset.\n")
                
                # Idle Heartbeat
                idle_duration = time.time() - last_data_time
                if idle_duration > 60 and (time.time() - last_heartbeat) > 60:
                    logger.info(f"💓 Spark Idle ({int(idle_duration)}s). Waiting for data...")
                    last_heartbeat = time.time()

                # Check for timeout (30s)
                if idle_duration > 30: 
                    status = check_pipeline_status()
                    
                    if status:
                        state = status.get("status")
                        
                        if state == "RUNNING":
                            # Producer is running but maybe slow. 
                            # We just wait. 
                            pass 

                        elif state == "SLEEPING":
                            # Producer is in deep sleep (rate limit)
                            wake_at = status.get("wake_at", 0)
                            wait_s = wake_at - time.time()
                            
                            if wait_s > 0:
                                logger.info(f"⏸ Synchronized Sleep: Waiting {int(wait_s/60)}m for Producer (Rate Limit)...")
                                # We can stop query to save resources or just sleep loop
                                # Sleeping loop keeps context alive. 
                                # Since we are just a consumer, sleeping is fine.
                                time.sleep(min(wait_s, 3600)) 
                                last_data_time = time.time() # Reset timer after wakeup

                        elif state == "COMPLETED":
                            logger.info("✅ Producer cycle COMPLETED. Spark will restart stream for next cycle.")
                            query.stop()
                            break
                    
                    else:
                        # Status file missing. Do NOT quit.
                        # Wait for producer to appear.
                        if (time.time() - last_heartbeat) > 60:
                            logger.warning(f"⚠️ Status file missing. Waiting for Producer... (Idle: {int(idle_duration)}s)")
                            last_heartbeat = time.time()
            
            query.awaitTermination()

        except Exception as e:
            logger.error(f"🔥 Critical Stream Error: {e}")
        
        # Short pause before restarting the stream (producer runs continuously now)
        logger.info("🔁 Restarting stream in 30 seconds...")
        time.sleep(30)


if __name__ == "__main__":
    main()