import json
import time
import os
import psycopg2
from psycopg2 import sql
from kafka import KafkaConsumer
from datetime import datetime

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'customer-heartbeats'
KAFKA_CONSUMER_GROUP = 'heartbeat-consumer-group'

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DATABASE = DB_NAME
PG_USER = POSTGRES_USER
PG_PASSWORD = POSTGRES_PASSWORD

# Validation thresholds
MIN_HEART_RATE = 30  # Lower than generator to catch most values
MAX_HEART_RATE = 220 # Higher than generator to catch most values

def create_kafka_consumer():
    """Create and return a Kafka consumer instance"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=False,  # Manual commit for better control
            max_poll_interval_ms=300000  # 5 minutes
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return consumer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def create_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print(f"Connected to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
        return conn
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return None

def is_valid_heart_rate(heart_rate):
    """Validate heart rate is within acceptable ranges"""
    return isinstance(heart_rate, int) and MIN_HEART_RATE <= heart_rate <= MAX_HEART_RATE

def is_valid_timestamp(timestamp_str):
    """Validate timestamp format"""
    try:
        datetime.fromisoformat(timestamp_str)
        return True
    except (ValueError, TypeError):
        return False

def validate_record(record):
    """Validate a heart rate record"""
    if not isinstance(record, dict):
        return False, "Record is not a dictionary"
    
    # Check required fields exist
    required_fields = ['customer_id', 'reading_time', 'heart_rate']
    for field in required_fields:
        if field not in record:
            return False, f"Missing required field: {field}"
    
    # Validate heart rate
    if not is_valid_heart_rate(record['heart_rate']):
        return False, f"Invalid heart rate: {record['heart_rate']}"
    
    # Validate timestamp
    if not is_valid_timestamp(record['reading_time']):
        return False, f"Invalid timestamp format: {record['reading_time']}"
    
    # Validate customer_id
    if not isinstance(record['customer_id'], str) or not record['customer_id']:
        return False, f"Invalid customer_id: {record['customer_id']}"
    
    return True, "Valid record"

def insert_to_db(conn, records):
    """Insert records into PostgreSQL database"""
    if not records:
        return 0
    
    insert_query = sql.SQL("""
        INSERT INTO customer_heartbeats (customer_id, reading_time, heart_rate)
        VALUES (%s, %s, %s)
    """)
    
    cursor = conn.cursor()
    inserted_count = 0
    
    try:
        for record in records:
            cursor.execute(
                insert_query, 
                (
                    record['customer_id'],
                    datetime.fromisoformat(record['reading_time']),
                    record['heart_rate']
                )
            )
            inserted_count += 1
        
        conn.commit()
        print(f"Inserted {inserted_count} records into the database")
    except Exception as e:
        conn.rollback()
        print(f"Database error: {e}")
    finally:
        cursor.close()
    
    return inserted_count

def process_messages(consumer, conn):
    """Process messages from Kafka and store in PostgreSQL"""
    try:
        # Poll for messages with a timeout
        messages = consumer.poll(timeout_ms=1000, max_records=100)
        
        valid_records = []
        processed_count = 0
        
        # Process each message
        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                processed_count += 1
                
                # Validate the record
                is_valid, validation_message = validate_record(message.value)
                
                if is_valid:
                    valid_records.append(message.value)
                else:
                    print(f"Invalid record: {validation_message}, Record: {message.value}")
        
        # Insert valid records into the database
        if valid_records:
            insert_to_db(conn, valid_records)
        
        # Commit offsets for processed messages
        consumer.commit()
        
        return processed_count
    except Exception as e:
        print(f"Error processing messages: {e}")
        return 0

def run_consumer():
    """Main function to run the heart beat data consumer"""
    consumer = create_kafka_consumer()
    if not consumer:
        print("Failed to create Kafka consumer. Exiting.")
        return
    
    conn = create_db_connection()
    if not conn:
        print("Failed to connect to PostgreSQL. Exiting.")
        if consumer:
            consumer.close()
        return
    
    print(f"Starting heart beat data consumer")
    print(f"Consuming from topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            processed_count = process_messages(consumer, conn)
            
            if processed_count == 0:
                # If no messages were processed, sleep briefly to avoid busy waiting
                time.sleep(0.1)
    except KeyboardInterrupt:
        print("Stopping heart beat data consumer")
    finally:
        # Close connections
        if consumer:
            consumer.close()
            print("Kafka consumer closed")
        
        if conn:
            conn.close()
            print("PostgreSQL connection closed")

if __name__ == "__main__":
    run_consumer()
