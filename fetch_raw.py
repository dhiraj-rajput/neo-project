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
    print("Waiting for services to be ready...")
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
            time.sleep(2)

def fetch_nasa_data(start_date, end_date):
    """Fetch data from NASA NEO API"""
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'api_key': NASA_API_KEY
    }
    
    print(f"Fetching data from {start_date} to {end_date}...")
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

def fetch_data_in_chunks(start_date_str, end_date_str):
    """Fetch data in 7-day chunks (API limit)"""
    start = datetime.strptime(start_date_str, '%Y-%m-%d')
    end = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    all_objects = []
    current_start = start
    
    while current_start <= end:
        current_end = min(current_start + timedelta(days=6), end)
        
        chunk_start = current_start.strftime('%Y-%m-%d')
        chunk_end = current_end.strftime('%Y-%m-%d')
        
        data = fetch_nasa_data(chunk_start, chunk_end)
        
        if data and 'near_earth_objects' in data:
            for date, asteroids in data['near_earth_objects'].items():
                all_objects.extend(asteroids)
        
        current_start = current_end + timedelta(days=1)
        time.sleep(1)  # Rate limiting
    
    return all_objects

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

def main():
    print("=" * 60)
    print("NASA NEO Data Fetcher & Kafka Producer")
    print("=" * 60)
    
    wait_for_services()
    
    # Initialize Kafka Producer
    print("\nInitializing Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=10485760,  # 10MB
        compression_type='gzip'
    )
    
    # Initialize DB connection
    print("Connecting to database...")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    
    try:
        # Fetch data
        print(f"\nFetching NEO data from {START_DATE} to {END_DATE}...")
        asteroids = fetch_data_in_chunks(START_DATE, END_DATE)
        print(f"✓ Fetched {len(asteroids)} asteroid records")
        
        # Process and send data
        print("\nProcessing and publishing data...")
        flattened_records = []
        sent_count = 0
        
        for asteroid in asteroids:
            flat = flatten_asteroid(asteroid)
            flattened_records.append(flat)
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, flat)
            sent_count += 1
            
            if sent_count % 100 == 0:
                print(f"  Sent {sent_count}/{len(asteroids)} records to Kafka")
        
        producer.flush()
        print(f"✓ Published {sent_count} records to Kafka topic: {KAFKA_TOPIC}")
        
        # Insert raw data into DB
        print("\nInserting raw data into database...")
        inserted = insert_raw_data(conn, flattened_records)
        print(f"✓ Inserted {inserted} records into neo_raw table")
        
        print("\n" + "=" * 60)
        print("SUCCESS: Data fetched, published to Kafka, and stored in DB")
        print("=" * 60)
        print(f"\nNext step: Run '2_kafka_consumer.py' to verify Kafka messages")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        raise
    finally:
        producer.close()
        conn.close()

if __name__ == "__main__":
    main()