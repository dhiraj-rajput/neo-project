"""
Step 3: Spark Structured Streaming - Process Kafka data and write to DB
Run: python 3_spark_processor.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import (
    col, from_json, when, lit, expr, mean, stddev, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType, LongType
)
import time

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'neo-raw-data')
DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'neo_db')
DB_USER = os.getenv('DB_USER', 'neo_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'neo_password')

# Schema for JSON data
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

def create_spark_session():
    """Create Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("NEO-Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def calculate_risk_score(df):
    """Calculate risk scores based on diameter, velocity, and miss distance"""
    
    # Get min/max for normalization
    stats = df.agg(
        mean("diameter_mean_km").alias("diam_mean"),
        stddev("diameter_mean_km").alias("diam_std"),
        mean("relative_velocity_km_s").alias("vel_mean"),
        stddev("relative_velocity_km_s").alias("vel_std"),
        mean("miss_distance_km").alias("miss_mean"),
        stddev("miss_distance_km").alias("miss_std")
    ).first()
    
    # Normalize features
    df = df.withColumn(
        "diameter_norm",
        (col("diameter_mean_km") - lit(stats["diam_mean"])) / lit(stats["diam_std"])
    ).withColumn(
        "velocity_norm",
        (col("relative_velocity_km_s") - lit(stats["vel_mean"])) / lit(stats["vel_std"])
    ).withColumn(
        "miss_distance_norm",
        -1 * (col("miss_distance_km") - lit(stats["miss_mean"])) / lit(stats["miss_std"])
    )
    
    # Calculate z-scores
    df = df.withColumn("diameter_zscore", col("diameter_norm"))
    df = df.withColumn("velocity_zscore", col("velocity_norm"))
    df = df.withColumn("miss_distance_zscore", col("miss_distance_norm"))
    
    # Risk score: weighted combination
    df = df.withColumn(
        "risk_score_base",
        (lit(0.4) * col("diameter_norm") + 
         lit(0.3) * col("velocity_norm") + 
         lit(0.3) * col("miss_distance_norm"))
    )
    
    # Boost if hazardous
    df = df.withColumn(
        "risk_score",
        when(col("is_potentially_hazardous") == True, 
             col("risk_score_base") + lit(0.5))
        .otherwise(col("risk_score_base"))
    )
    
    # Clip to 0-1 range
    df = df.withColumn(
        "risk_score",
        when(col("risk_score") < 0, 0)
        .when(col("risk_score") > 1, 1)
        .otherwise(col("risk_score"))
    )
    
    # Risk level categories
    df = df.withColumn(
        "risk_level",
        when(col("risk_score") < 0.2, "Very Low")
        .when(col("risk_score") < 0.4, "Low")
        .when(col("risk_score") < 0.6, "Medium")
        .when(col("risk_score") < 0.8, "High")
        .otherwise("Very High")
    )
    
    # Flag anomalies (|z-score| > 2)
    df = df.withColumn(
        "is_anomaly",
        (spark_abs(col("diameter_zscore")) > 2) | 
        (spark_abs(col("velocity_zscore")) > 2) | 
        (spark_abs(col("miss_distance_zscore")) > 2)
    )
    
    return df

def write_to_postgres(df, epoch_id):
    """Write batch to PostgreSQL"""
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    # Select columns for neo_processed table
    processed_df = df.select(
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
    
    processed_df.write \
        .jdbc(url=jdbc_url, table="neo_processed", mode="append", properties=properties)
    
    print(f"Batch {epoch_id}: Written {df.count()} records to neo_processed")

def main():
    print("=" * 60)
    print("Spark Structured Streaming Processor")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\nReading from Kafka...")
    print(f"  Bootstrap Servers: {KAFKA_BOOTSTRAP}")
    print(f"  Topic: {KAFKA_TOPIC}")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Calculate mean diameter
    parsed_df = parsed_df.withColumn(
        "diameter_mean_km",
        (col("estimated_diameter_km_min") + col("estimated_diameter_km_max")) / 2
    )
    
    # Process in batches
    print("\nProcessing stream (Ctrl+C to stop)...")
    print("Calculating risk scores and detecting anomalies...\n")
    
    query = parsed_df \
        .writeStream \
        .foreachBatch(lambda batch_df, epoch_id: process_batch(batch_df, epoch_id)) \
        .outputMode("append") \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping stream...")
        query.stop()
        spark.stop()
        print("\n" + "=" * 60)
        print("SUCCESS: Stream processing stopped")
        print("=" * 60)
        print("\nNext step: Run '4_calculate_daily_metrics.py' to aggregate data")

def process_batch(batch_df, epoch_id):
    """Process each batch"""
    if batch_df.count() == 0:
        return
    
    print(f"\n--- Batch {epoch_id} ---")
    print(f"Records: {batch_df.count()}")
    
    # Calculate risk scores
    processed_df = calculate_risk_score(batch_df)
    
    # ⚡ Convert date string to timestamp for PostgreSQL
    processed_df = processed_df.withColumn(
        "close_approach_date",
        to_timestamp(col("close_approach_date"), "yyyy-MM-dd")
    )
    
    # Write to PostgreSQL
    write_to_postgres(processed_df, epoch_id)
    
    # Show sample
    print("\nSample processed records:")
    processed_df.select(
        "name", "risk_score", "risk_level", "is_anomaly"
    ).show(5, truncate=False)


if __name__ == "__main__":
    main()