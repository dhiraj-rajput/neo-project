"""
Step 2: Kafka Consumer to verify messages (optional check)
Run: python kafka_producer.py
"""
import os
import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'neo-raw-data')

def main():
    print("=" * 60)
    print("Kafka Consumer - Message Verification")
    print("=" * 60)
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    print(f"\nConsuming from topic: {KAFKA_TOPIC}")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    try:
        for message in consumer:
            count += 1
            data = message.value
            print(f"Message {count}:")
            print(f"  ID: {data.get('id')}")
            print(f"  Name: {data.get('name')}")
            print(f"  Date: {data.get('close_approach_date')}")
            print(f"  Hazardous: {data.get('is_potentially_hazardous')}")
            print(f"  Miss Distance: {data.get('miss_distance_km')} km")
            print("-" * 60)
            
            if count >= 10:
                print(f"\nShowing first 10 messages. Total consumed: {count}")
                break
                
    except KeyboardInterrupt:
        print(f"\n\nStopped. Total messages consumed: {count}")
    except Exception as e:
        print(f"\nNo more messages or timeout. Total consumed: {count}")
    finally:
        consumer.close()
    
    print("\n" + "=" * 60)
    print(f"SUCCESS: Verified {count} messages from Kafka")
    print("=" * 60)
    print("\nNext step: Run '3_spark_processor.py' to process data")

if __name__ == "__main__":
    main()