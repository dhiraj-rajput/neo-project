"""
Step 1: Fetch NASA NEO data and produce to Kafka & store raw data in DB
Run: python fetch_raw.py
"""
import os
import json
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import execute_values
import time

# Configuration
NASA_API_KEY = os.getenv('NASA_API_KEY', 'DEMO_KEY')
START_DATE = os.getenv('START_DATE', '2024-01-01')
END_DATE = os.getenv('END_DATE', '2024-01-07')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'neo-raw-data')
DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'neo_db')
DB_USER = os.getenv('DB_USER', 'neo_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'neo_password')

def wait_for_services():
    """Wait for Kafka and DB to be ready"""
    print("⏳ Waiting for services to be ready...")
    max_retries = 30
    
    # Wait for Kafka
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            producer.close()
            print("✓ Kafka is ready")
            break
        except Exception as e:
            if i == max_retries - 1:
                raise Exception(f"Kafka not ready after {max_retries} attempts")
            print(f"  Waiting for Kafka... ({i+1}/{max_retries})")
            time.sleep(2)
    
    # Wait for DB
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD, connect_timeout=5
            )
            conn.close()
            print("✓ Database is ready")
            break
        except Exception as e:
            if i == max_retries - 1:
                raise Exception(f"Database not ready after {max_retries} attempts")
            print(f"  Waiting for Database... ({i+1}/{max_retries})")
            time.sleep(2)

def fetch_nasa_data(start_date, end_date):
    """Fetch data from NASA NEO API"""
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'api_key': NASA_API_KEY
    }
    
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

def flatten_asteroid(asteroid):
    """Flatten nested asteroid structure"""
    flat = {
        'id': asteroid.get('id'),
        'neo_reference_id': asteroid.get('neo_reference_id'),
        'name': asteroid.get('name'),
        'nasa_jpl_url': asteroid.get('nasa_jpl_url'),
        'absolute_magnitude_h': asteroid.get('absolute_magnitude_h'),
        'is_potentially_hazardous': asteroid.get('is_potentially_hazardous_asteroid', False),
        'is_sentry_object': asteroid.get('is_sentry_object', False)
    }
    
    # Diameter data
    if 'estimated_diameter' in asteroid:
        ed = asteroid['estimated_diameter']
        if 'kilometers' in ed:
            flat['estimated_diameter_km_min'] = ed['kilometers'].get('estimated_diameter_min')
            flat['estimated_diameter_km_max'] = ed['kilometers'].get('estimated_diameter_max')
        if 'meters' in ed:
            flat['estimated_diameter_m_min'] = ed['meters'].get('estimated_diameter_min')
            flat['estimated_diameter_m_max'] = ed['meters'].get('estimated_diameter_max')
        if 'miles' in ed:
            flat['estimated_diameter_miles_min'] = ed['miles'].get('estimated_diameter_min')
            flat['estimated_diameter_miles_max'] = ed['miles'].get('estimated_diameter_max')
        if 'feet' in ed:
            flat['estimated_diameter_feet_min'] = ed['feet'].get('estimated_diameter_min')
            flat['estimated_diameter_feet_max'] = ed['feet'].get('estimated_diameter_max')
    
    # Close approach data
    if 'close_approach_data' in asteroid and asteroid['close_approach_data']:
        approach = asteroid['close_approach_data'][0]
        flat['close_approach_date'] = approach.get('close_approach_date')
        flat['close_approach_date_full'] = approach.get('close_approach_date_full')
        flat['epoch_date_close_approach'] = approach.get('epoch_date_close_approach')
        flat['orbiting_body'] = approach.get('orbiting_body')
        
        if 'relative_velocity' in approach:
            rv = approach['relative_velocity']
            flat['relative_velocity_km_s'] = float(rv.get('kilometers_per_second', 0))
            flat['relative_velocity_km_h'] = float(rv.get('kilometers_per_hour', 0))
            flat['relative_velocity_mph'] = float(rv.get('miles_per_hour', 0))
        
        if 'miss_distance' in approach:
            md = approach['miss_distance']
            flat['miss_distance_astronomical'] = float(md.get('astronomical', 0))
            flat['miss_distance_lunar'] = float(md.get('lunar', 0))
            flat['miss_distance_km'] = float(md.get('kilometers', 0))
            flat['miss_distance_miles'] = float(md.get('miles', 0))
    
    return flat

def insert_raw_data(conn, records):
    """Insert raw data into database"""
    if not records:
        return 0
    
    columns = [
        'id', 'neo_reference_id', 'name', 'nasa_jpl_url', 'absolute_magnitude_h',
        'is_potentially_hazardous', 'is_sentry_object', 'close_approach_date',
        'close_approach_date_full', 'epoch_date_close_approach', 'relative_velocity_km_s',
        'relative_velocity_km_h', 'relative_velocity_mph', 'miss_distance_astronomical',
        'miss_distance_lunar', 'miss_distance_km', 'miss_distance_miles', 'orbiting_body',
        'estimated_diameter_km_min', 'estimated_diameter_km_max', 'estimated_diameter_m_min',
        'estimated_diameter_m_max', 'estimated_diameter_miles_min', 'estimated_diameter_miles_max',
        'estimated_diameter_feet_min', 'estimated_diameter_feet_max'
    ]
    
    values = []
    for rec in records:
        values.append(tuple(rec.get(col) for col in columns))
    
    insert_query = f"""
        INSERT INTO neo_raw ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (id, close_approach_date) DO NOTHING
    """
    
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values)
        conn.commit()
    
    return len(values)

def process_and_stream_chunk(producer, conn, chunk_start, chunk_end, total_stats):
    """Fetch, process, and stream data for a date range chunk"""
    print(f"\n📅 Processing date range: {chunk_start} to {chunk_end}")
    print(f"   ⬇️  Fetching data from NASA API...")
    
    # Fetch data from NASA
    data = fetch_nasa_data(chunk_start, chunk_end)
    
    if not data or 'near_earth_objects' not in data:
        print(f"   ⚠️  No data returned for this range")
        return
    
    # Process each date's data
    chunk_total = 0
    chunk_records = []
    
    for date, asteroids in sorted(data['near_earth_objects'].items()):
        date_count = len(asteroids)
        chunk_total += date_count
        
        print(f"   📊 {date}: Found {date_count} asteroids")
        
        # Process and stream each asteroid immediately
        for i, asteroid in enumerate(asteroids, 1):
            flat = flatten_asteroid(asteroid)
            chunk_records.append(flat)
            
            # Send to Kafka immediately
            try:
                producer.send(KAFKA_TOPIC, flat)
                total_stats['kafka_sent'] += 1
            except Exception as e:
                print(f"      ✗ Failed to send to Kafka: {str(e)}")
                total_stats['kafka_failed'] += 1
        
        print(f"      ✓ Streamed {date_count} records to Kafka")
    
    # Flush Kafka producer to ensure all messages are sent
    producer.flush()
    print(f"   ✓ Kafka flush completed")
    
    # Batch insert to database for this chunk
    if chunk_records:
        print(f"   💾 Inserting {len(chunk_records)} records to database...")
        inserted = insert_raw_data(conn, chunk_records)
        total_stats['db_inserted'] += inserted
        print(f"   ✓ Inserted {inserted} records to database")
    
    total_stats['total_asteroids'] += chunk_total
    print(f"   ✅ Chunk complete: {chunk_total} asteroids processed")

def main():
    print("=" * 70)
    print("🚀 NASA NEO Data Fetcher & Real-Time Kafka Streaming Producer")
    print("=" * 70)
    
    # Wait for services
    wait_for_services()
    
    # Initialize Kafka Producer
    print("\n🔧 Initializing Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=10485760,  # 10MB
        compression_type='gzip',
        acks='all',  # Wait for all replicas
        retries=3
    )
    print("✓ Kafka producer initialized")
    
    # Initialize DB connection
    print("🔧 Connecting to database...")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    print("✓ Database connection established")
    
    # Statistics tracking
    total_stats = {
        'total_asteroids': 0,
        'kafka_sent': 0,
        'kafka_failed': 0,
        'db_inserted': 0
    }
    
    try:
        # Calculate date range
        start = datetime.strptime(START_DATE, '%Y-%m-%d')
        end = datetime.strptime(END_DATE, '%Y-%m-%d')
        total_days = (end - start).days + 1
        
        print(f"\n📋 Configuration:")
        print(f"   Date Range: {START_DATE} to {END_DATE} ({total_days} days)")
        print(f"   Kafka Topic: {KAFKA_TOPIC}")
        print(f"   API Key: {'DEMO_KEY' if NASA_API_KEY == 'DEMO_KEY' else 'Custom Key'}")
        
        print(f"\n{'='*70}")
        print("🔄 Starting real-time data streaming...")
        print(f"{'='*70}")
        
        # Process data in 7-day chunks (API limit)
        current_start = start
        chunk_number = 0
        
        while current_start <= end:
            chunk_number += 1
            current_end = min(current_start + timedelta(days=6), end)
            
            chunk_start_str = current_start.strftime('%Y-%m-%d')
            chunk_end_str = current_end.strftime('%Y-%m-%d')
            
            print(f"\n{'─'*70}")
            print(f"📦 Chunk {chunk_number}")
            
            # Process and stream this chunk
            process_and_stream_chunk(
                producer, conn, 
                chunk_start_str, chunk_end_str, 
                total_stats
            )
            
            # Move to next chunk
            current_start = current_end + timedelta(days=1)
            
            # Rate limiting between chunks
            if current_start <= end:
                print(f"   ⏸️  Rate limiting: waiting 1 second...")
                time.sleep(1)
        
        # Final summary
        print(f"\n{'='*70}")
        print("✅ SUCCESS: All data processed and streamed!")
        print(f"{'='*70}")
        print(f"\n📊 Final Statistics:")
        print(f"   Total Asteroids Processed: {total_stats['total_asteroids']}")
        print(f"   Kafka Messages Sent: {total_stats['kafka_sent']}")
        print(f"   Kafka Messages Failed: {total_stats['kafka_failed']}")
        print(f"   Database Records Inserted: {total_stats['db_inserted']}")
        print(f"   Kafka Topic: {KAFKA_TOPIC}")
        
        if total_stats['kafka_failed'] > 0:
            print(f"\n⚠️  Warning: {total_stats['kafka_failed']} messages failed to send to Kafka")
        
        print(f"\n{'='*70}")
        print("📌 Next Steps:")
        print("   1. Run 'python kafka_consumer.py' to verify Kafka messages")
        print("   2. Check database: SELECT COUNT(*) FROM neo_raw;")
        print(f"{'='*70}\n")
        
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Process interrupted by user")
        print(f"📊 Statistics at interruption:")
        print(f"   Asteroids: {total_stats['total_asteroids']}")
        print(f"   Kafka Sent: {total_stats['kafka_sent']}")
        print(f"   DB Inserted: {total_stats['db_inserted']}")
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"✗ ERROR: {str(e)}")
        print(f"{'='*70}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        print("\n🔧 Cleaning up...")
        producer.close()
        conn.close()
        print("✓ Connections closed")

if __name__ == "__main__":
    main()