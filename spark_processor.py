"""
Step 3: Spark Structured Streaming - Process Kafka data continuously
Run: python 3_spark_processor.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp, col, from_json, when, lit, mean, stddev, 
    abs as spark_abs, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType
import time

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'neo-raw-data')
DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'neo_db')
DB_USER = os.getenv('DB_USER', 'neo_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'neo_password')
CHECKPOINT_DIR = os.getenv('CHECKPOINT_DIR', '/tmp/spark-checkpoint')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
TRIGGER_INTERVAL = os.getenv('TRIGGER_INTERVAL', '30 seconds')

schema = StructType([
    StructField("id", StringType(), True),
    StructField("neo_reference_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("is_potentially_hazardous", BooleanType(), True),
    StructField("close_approach_date", StringType(), True),
    StructField("relative_velocity_km_s", FloatType(), True),
    StructField("miss_distance_km", FloatType(), True),
    StructField("estimated_diameter_km_min", FloatType(), True),
    StructField("estimated_diameter_km_max", FloatType(), True)
])

batch_counter = {"count": 0, "total_records": 0}

def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("NEO-StreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

def calculate_risk_score(df):
    """Calculate risk scores based on diameter, velocity, and miss distance"""
    
    stats = df.agg(
        mean("diameter_mean_km").alias("diam_mean"),
        stddev("diameter_mean_km").alias("diam_std"),
        mean("relative_velocity_km_s").alias("vel_mean"),
        stddev("relative_velocity_km_s").alias("vel_std"),
        mean("miss_distance_km").alias("miss_mean"),
        stddev("miss_distance_km").alias("miss_std")
    ).first()
    
    # Handle edge case: if stddev is 0 or None, set to 1 to avoid division by zero
    diam_std = stats["diam_std"] if stats["diam_std"] and stats["diam_std"] > 0 else 1.0
    vel_std = stats["vel_std"] if stats["vel_std"] and stats["vel_std"] > 0 else 1.0
    miss_std = stats["miss_std"] if stats["miss_std"] and stats["miss_std"] > 0 else 1.0
    
    # Normalize features (Z-score normalization)
    df = df.withColumn(
        "diameter_norm",
        (col("diameter_mean_km") - lit(stats["diam_mean"])) / lit(diam_std)
    ).withColumn(
        "velocity_norm",
        (col("relative_velocity_km_s") - lit(stats["vel_mean"])) / lit(vel_std)
    ).withColumn(
        "miss_distance_norm",
        -1 * (col("miss_distance_km") - lit(stats["miss_mean"])) / lit(miss_std)
    )
    
    # Store z-scores for anomaly detection
    df = df.withColumn("diameter_zscore", col("diameter_norm"))
    df = df.withColumn("velocity_zscore", col("velocity_norm"))
    df = df.withColumn("miss_distance_zscore", col("miss_distance_norm"))
    
    # Calculate base risk score (weighted combination)
    df = df.withColumn(
        "risk_score_base",
        (lit(0.4) * col("diameter_norm") + 
         lit(0.3) * col("velocity_norm") + 
         lit(0.3) * col("miss_distance_norm"))
    )
    
    df = df.withColumn(
        "risk_score",
        when(col("is_potentially_hazardous") == True, 
             col("risk_score_base") + lit(0.5))
        .otherwise(col("risk_score_base"))
    )
    
    # Clamp risk score between 0 and 1
    df = df.withColumn(
        "risk_score",
        when(col("risk_score") < 0, 0)
        .when(col("risk_score") > 1, 1)
        .otherwise(col("risk_score"))
    )
    
    # Assign risk level categories
    df = df.withColumn(
        "risk_level",
        when(col("risk_score") < 0.2, "Very Low")
        .when(col("risk_score") < 0.4, "Low")
        .when(col("risk_score") < 0.6, "Medium")
        .when(col("risk_score") < 0.8, "High")
        .otherwise("Very High")
    )
    
    # Detect anomalies (Z-score > 2 or < -2)
    df = df.withColumn(
        "is_anomaly",
        (spark_abs(col("diameter_zscore")) > 2) | 
        (spark_abs(col("velocity_zscore")) > 2) | 
        (spark_abs(col("miss_distance_zscore")) > 2)
    )
    
    return df

def process_batch(batch_df, batch_id):
    """Process each micro-batch from the stream"""
    
    batch_counter["count"] += 1
    current_batch = batch_counter["count"]
    
    print(f"\n{'='*70}")
    print(f"🔄 Processing Batch #{current_batch} (Spark Batch ID: {batch_id})")
    print(f"{'='*70}")
    
    # Check if batch is empty
    if batch_df.isEmpty():
        print(f"⚠️  Batch #{current_batch} is empty - no new messages")
        return
    
    try:
        # Count records in this batch
        record_count = batch_df.count()
        batch_counter["total_records"] += record_count
        
        print(f"📊 Records in this batch: {record_count}")
        print(f"📈 Total records processed so far: {batch_counter['total_records']}")
        
        # Calculate mean diameter
        batch_df = batch_df.withColumn(
            "diameter_mean_km",
            (col("estimated_diameter_km_min") + col("estimated_diameter_km_max")) / 2
        )
        
        # Calculate risk scores
        print(f"🧮 Calculating risk scores...")
        processed_df = calculate_risk_score(batch_df)
        
        # Convert date string to timestamp
        processed_df = processed_df.withColumn(
            "close_approach_date",
            to_timestamp(col("close_approach_date"), "yyyy-MM-dd")
        )
        
        # Add processing timestamp
        processed_df = processed_df.withColumn(
            "processed_at",
            current_timestamp()
        )
        
        # Select columns for database
        final_df = processed_df.select(
            "id",
            "name",
            "close_approach_date",
            "is_potentially_hazardous",
            "diameter_mean_km",
            "miss_distance_km",
            "relative_velocity_km_s",
            "risk_score",
            "risk_level",
            "diameter_zscore",
            "velocity_zscore",
            "miss_distance_zscore",
            "is_anomaly"
        )
        
        # Write to PostgreSQL
        print(f"💾 Writing to database...")
        jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
        properties = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        final_df.write \
            .jdbc(url=jdbc_url, table="neo_processed", mode="append", properties=properties)
        
        print(f"✅ Batch #{current_batch}: Successfully written {record_count} records to neo_processed")
        
        # Show statistics for this batch
        anomaly_count = processed_df.filter(col("is_anomaly") == True).count()
        high_risk_count = processed_df.filter(col("risk_level").isin(["High", "Very High"])).count()
        hazardous_count = processed_df.filter(col("is_potentially_hazardous") == True).count()
        
        print(f"\n📊 Batch Statistics:")
        print(f"   🚨 Anomalies detected: {anomaly_count}")
        print(f"   ⚠️  High/Very High risk: {high_risk_count}")
        print(f"   ☢️  Potentially hazardous: {hazardous_count}")
        
        # Show sample records
        print(f"\n📋 Sample records from this batch:")
        processed_df.select(
            "name", "risk_score", "risk_level", "is_anomaly", "is_potentially_hazardous"
        ).show(5, truncate=False)
        
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"❌ Error processing batch #{current_batch}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def main():
    print("=" * 70)
    print("🚀 Spark Structured Streaming Processor")
    print("=" * 70)
    
    print(f"\n📋 Configuration:")
    print(f"   Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Max Records per Batch: {BATCH_SIZE}")
    print(f"   Trigger Interval: {TRIGGER_INTERVAL}")
    print(f"   Checkpoint Directory: {CHECKPOINT_DIR}")
    print(f"   Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    print(f"\n🔧 Initializing Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark session created")
    
    print(f"\n{'='*70}")
    print(f"📡 Starting Kafka stream consumption...")
    print(f"{'='*70}")
    
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", BATCH_SIZE) \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"✅ Kafka stream connected")
    
    parsed_stream = kafka_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    print(f"\n🔄 Starting stream processing...")
    print(f"   Press Ctrl+C to stop gracefully")
    print(f"\n{'='*70}\n")
    
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print(f"\n\n{'='*70}")
        print(f"⚠️  Stream processing interrupted by user")
        print(f"{'='*70}")
        print(f"\n📊 Final Statistics:")
        print(f"   Total Batches Processed: {batch_counter['count']}")
        print(f"   Total Records Processed: {batch_counter['total_records']}")
        print(f"\n✅ Stream stopped gracefully")
        query.stop()
    finally:
        spark.stop()
        print(f"🔧 Spark session closed\n")

if __name__ == "__main__":
    main()