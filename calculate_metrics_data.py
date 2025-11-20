import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date,
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    lit,
)

# Environment config
DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "neo_db")
DB_USER = os.getenv("DB_USER", "neo_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "neo_password")
REFRESH_INTERVAL = int(os.getenv("METRICS_REFRESH_INTERVAL", 60))

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def create_spark():
    """Create a Spark session."""
    return (
        SparkSession.builder.appName("NEO-Daily-Metrics-Aggregation")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )


def calculate_daily_metrics(spark):
    """Load data using Spark, aggregate, write results."""
    print("📥 Loading neo_processed from database...")

    df = spark.read.jdbc(url=JDBC_URL, table="neo_processed", properties=DB_PROPERTIES)

    if df.count() == 0:
        print("⚠️ No processed data found.")
        return None

    print("🔄 Running daily aggregations using Spark...")

    df = df.withColumn("metric_date", to_date(col("close_approach_date")))

    agg_df = (
        df.groupBy("metric_date")
        .agg(
            count("*").alias("asteroid_count"),
            spark_sum(col("is_potentially_hazardous").cast("int")).alias(
                "hazardous_count"
            ),
            avg("diameter_mean_km").alias("avg_diameter_km"),
            spark_max("diameter_mean_km").alias("max_diameter_km"),
            avg("relative_velocity_km_s").alias("avg_velocity_km_s"),
            spark_max("relative_velocity_km_s").alias("max_velocity_km_s"),
            spark_min("miss_distance_km").alias("min_miss_distance_km"),
            avg("miss_distance_km").alias("avg_miss_distance_km"),
            avg("risk_score").alias("avg_risk_score"),
            spark_sum((col("risk_score") > 0.6).cast("int")).alias("high_risk_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn(
            "calculated_time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
    )

    print("💾 Writing daily metrics back to database...")

    # Overwrite entire table OR change to mode="append"
    agg_df.write.jdbc(
        url=JDBC_URL,
        table="neo_daily_metrics",
        mode="overwrite",  # or "append" depending on your strategy
        properties=DB_PROPERTIES,
    )

    print("✅ Daily metrics computed and stored successfully.")

    return agg_df


def main():
    print("=" * 70)
    print("🚀 Spark Daily Metrics Calculator (Continuous Mode)")
    print("=" * 70)

    spark = create_spark()

    try:
        while True:
            print("\n🔄 Starting new aggregation cycle...")
            calculate_daily_metrics(spark)

            print(f"⏳ Sleeping for {REFRESH_INTERVAL} seconds...\n")
            time.sleep(REFRESH_INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 Stopping gracefully...")
    finally:
        spark.stop()
        print("✨ Spark stopped.")


if __name__ == "__main__":
    main()
