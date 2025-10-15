"""
Step 4: Calculate daily aggregated metrics continuously
Run: python 4_calculate_daily_metrics.py
"""
import os
import psycopg2
from datetime import datetime
import time
import signal
import sys

DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'neo_db')
DB_USER = os.getenv('DB_USER', 'neo_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'neo_password')
REFRESH_INTERVAL = int(os.getenv('METRICS_REFRESH_INTERVAL', 60))  # seconds

# Global flag for graceful shutdown
running = True
execution_stats = {
    'total_runs': 0,
    'successful_runs': 0,
    'failed_runs': 0,
    'total_days_processed': 0
}

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global running
    print(f"\n\n{'='*70}")
    print(f"⚠️  Shutdown signal received - stopping gracefully...")
    print(f"{'='*70}")
    running = False

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
    
    try:
        with conn.cursor() as cur:
            # Execute aggregation query
            cur.execute(query)
            rows_affected = cur.rowcount
            conn.commit()
            
            # Get total metrics count and latest stats
            cur.execute("""
                SELECT 
                    COUNT(*) as total_days,
                    SUM(asteroid_count) as total_asteroids,
                    SUM(hazardous_count) as total_hazardous,
                    SUM(anomaly_count) as total_anomalies
                FROM neo_daily_metrics
            """)
            stats = cur.fetchone()
            
            # Get date range
            cur.execute("""
                SELECT 
                    MIN(metric_date) as earliest_date,
                    MAX(metric_date) as latest_date
                FROM neo_daily_metrics
            """)
            date_range = cur.fetchone()
        
        return {
            'rows_affected': rows_affected,
            'total_days': stats[0] if stats else 0,
            'total_asteroids': stats[1] if stats else 0,
            'total_hazardous': stats[2] if stats else 0,
            'total_anomalies': stats[3] if stats else 0,
            'earliest_date': date_range[0] if date_range else None,
            'latest_date': date_range[1] if date_range else None
        }
    finally:
        conn.close()

def main():
    global running
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 70)
    print("🔄 Daily Metrics Calculator - Continuous Mode")
    print("=" * 70)
    
    print(f"\n📋 Configuration:")
    print(f"   Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"   Refresh Interval: {REFRESH_INTERVAL} seconds")
    print(f"   Press Ctrl+C to stop gracefully")
    
    print(f"\n{'='*70}")
    print(f"🚀 Starting continuous metric calculation...")
    print(f"{'='*70}\n")
    
    while running:
        try:
            execution_stats['total_runs'] += 1
            run_number = execution_stats['total_runs']
            
            # Log execution start
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n{'─'*70}")
            print(f"📊 Execution #{run_number} - {timestamp}")
            print(f"{'─'*70}")
            
            print(f"🔄 Calculating daily metrics...")
            start_time = time.time()
            
            # Calculate metrics
            result = calculate_daily_metrics()
            
            elapsed_time = time.time() - start_time
            execution_stats['successful_runs'] += 1
            execution_stats['total_days_processed'] = result['total_days']
            
            # Log results
            print(f"✅ Metrics calculated successfully in {elapsed_time:.2f}s")
            print(f"\n📈 Metrics Summary:")
            print(f"   Days with metrics: {result['total_days']}")
            print(f"   Total asteroids: {result['total_asteroids']}")
            print(f"   Potentially hazardous: {result['total_hazardous']}")
            print(f"   Anomalies detected: {result['total_anomalies']}")
            
            if result['earliest_date'] and result['latest_date']:
                print(f"\n📅 Date Range:")
                print(f"   Earliest: {result['earliest_date']}")
                print(f"   Latest: {result['latest_date']}")
            
            print(f"\n📊 Execution Statistics:")
            print(f"   Total runs: {execution_stats['total_runs']}")
            print(f"   Successful: {execution_stats['successful_runs']}")
            print(f"   Failed: {execution_stats['failed_runs']}")
            
            # Wait for next execution if still running
            if running:
                print(f"\n⏸️  Waiting {REFRESH_INTERVAL} seconds until next calculation...")
                print(f"   Next run scheduled at: {datetime.fromtimestamp(time.time() + REFRESH_INTERVAL).strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Sleep in small increments to allow for quick shutdown
                for _ in range(REFRESH_INTERVAL):
                    if not running:
                        break
                    time.sleep(1)
        
        except psycopg2.Error as db_error:
            execution_stats['failed_runs'] += 1
            print(f"\n❌ Database error: {str(db_error)}")
            print(f"   Retrying in {REFRESH_INTERVAL} seconds...")
            
            # Wait before retry
            for _ in range(REFRESH_INTERVAL):
                if not running:
                    break
                time.sleep(1)
        
        except Exception as e:
            execution_stats['failed_runs'] += 1
            print(f"\n❌ Unexpected error: {str(e)}")
            import traceback
            traceback.print_exc()
            print(f"   Retrying in {REFRESH_INTERVAL} seconds...")
            
            # Wait before retry
            for _ in range(REFRESH_INTERVAL):
                if not running:
                    break
                time.sleep(1)
    
    # Final summary after graceful shutdown
    print(f"\n{'='*70}")
    print(f"📊 Final Execution Summary")
    print(f"{'='*70}")
    print(f"   Total Executions: {execution_stats['total_runs']}")
    print(f"   Successful: {execution_stats['successful_runs']}")
    print(f"   Failed: {execution_stats['failed_runs']}")
    print(f"   Days Processed: {execution_stats['total_days_processed']}")
    
    if execution_stats['total_runs'] > 0:
        success_rate = (execution_stats['successful_runs'] / execution_stats['total_runs']) * 100
        print(f"   Success Rate: {success_rate:.1f}%")
    
    print(f"\n✅ Metrics calculator stopped gracefully")
    print(f"{'='*70}")
    print(f"\n💡 Next step: Run 'python 5_streamlit_ui.py' to view dashboard\n")

if __name__ == "__main__":
    main()