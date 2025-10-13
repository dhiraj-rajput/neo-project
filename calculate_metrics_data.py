"""
Step 4: Calculate daily aggregated metrics
Run: python 4_calculate_daily_metrics.py
"""
import os
import psycopg2
from datetime import datetime

DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'neo_db')
DB_USER = os.getenv('DB_USER', 'neo_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'neo_password')

def calculate_daily_metrics():
    """Calculate and store daily aggregated metrics"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    
    query = """
    INSERT INTO neo_daily_metrics (
        metric_date,
        asteroid_count,
        hazardous_count,
        avg_diameter_km,
        max_diameter_km,
        avg_velocity_km_s,
        max_velocity_km_s,
        min_miss_distance_km,
        avg_miss_distance_km,
        avg_risk_score,
        high_risk_count,
        anomaly_count
    )
    SELECT 
        DATE(close_approach_date) as metric_date,
        COUNT(*) as asteroid_count,
        SUM(CASE WHEN is_potentially_hazardous THEN 1 ELSE 0 END) as hazardous_count,
        AVG(diameter_mean_km) as avg_diameter_km,
        MAX(diameter_mean_km) as max_diameter_km,
        AVG(relative_velocity_km_s) as avg_velocity_km_s,
        MAX(relative_velocity_km_s) as max_velocity_km_s,
        MIN(miss_distance_km) as min_miss_distance_km,
        AVG(miss_distance_km) as avg_miss_distance_km,
        AVG(risk_score) as avg_risk_score,
        SUM(CASE WHEN risk_score > 0.6 THEN 1 ELSE 0 END) as high_risk_count,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
    FROM neo_processed
    GROUP BY DATE(close_approach_date)
    ON CONFLICT (metric_date) DO UPDATE SET
        asteroid_count = EXCLUDED.asteroid_count,
        hazardous_count = EXCLUDED.hazardous_count,
        avg_diameter_km = EXCLUDED.avg_diameter_km,
        max_diameter_km = EXCLUDED.max_diameter_km,
        avg_velocity_km_s = EXCLUDED.avg_velocity_km_s,
        max_velocity_km_s = EXCLUDED.max_velocity_km_s,
        min_miss_distance_km = EXCLUDED.min_miss_distance_km,
        avg_miss_distance_km = EXCLUDED.avg_miss_distance_km,
        avg_risk_score = EXCLUDED.avg_risk_score,
        high_risk_count = EXCLUDED.high_risk_count,
        anomaly_count = EXCLUDED.anomaly_count,
        calculated_time = NOW();
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()
        
        # Get summary
        cur.execute("SELECT COUNT(*) FROM neo_daily_metrics")
        count = cur.fetchone()[0]
    
    conn.close()
    return count

def main():
    print("=" * 60)
    print("Daily Metrics Calculator")
    print("=" * 60)
    
    print("\nCalculating daily aggregated metrics...")
    count = calculate_daily_metrics()
    
    print(f"✓ Calculated metrics for {count} days")
    
    print("\n" + "=" * 60)
    print("SUCCESS: Daily metrics calculated")
    print("=" * 60)
    print("\nNext step: Run '5_streamlit_ui.py' to view dashboard")

if __name__ == "__main__":
    main()