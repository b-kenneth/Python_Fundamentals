import json
import time
import os
import psycopg2
from psycopg2 import sql
from utils import wait_for_kafka, wait_for_postgres, setup_logging
from kafka import KafkaConsumer
from datetime import datetime

# Setup logging
logger = setup_logging('heartbeat-consumer', log_file='logs\heartbeat_consumer.log')

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

KAFKA_TOPIC = 'customer-heartbeats'
KAFKA_CONSUMER_GROUP = 'heartbeat-consumer-group'

# PostgreSQL configuration
PG_HOST = os.environ.get('PG_HOST', 'localhost')
PG_PORT = int(os.environ.get('PG_PORT', '5432'))
PG_DATABASE = os.environ.get('PG_DATABASE')
PG_USER = os.environ.get('PG_USER')
PG_PASSWORD = os.environ.get('PG_PASSWORD')

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
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}", exc_info=True)
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
        logger.info(f"Connected to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
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
        logger.info(f"Inserted {inserted_count} records into the database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}", exc_info=True)
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
        error_count = 0
        
        # Process each message
        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                processed_count += 1
                
                # Validate the record
                is_valid, validation_message = validate_record(message.value)
                
                if is_valid:
                    valid_records.append(message.value)
                    logger.debug(f"Valid record processed: {message.value}")
                else:
                    error_count += 1
                    logger.warning(f"Invalid record: {validation_message}, Record: {message.value}")
        
        # Insert valid records into the database
        if valid_records:
            insert_to_db(conn, valid_records)
        
        # Commit offsets for processed messages
        consumer.commit()
        
        if processed_count > 0:
            logger.info(f"Processed batch: {processed_count} messages, {len(valid_records)} valid, {error_count} invalid")
        
        return processed_count
    except Exception as e:
        logger.error(f"Error processing messages: {e}", exc_info=True)
        return 0

def run_consumer():
    """Main function to run the heart beat data consumer"""
    logger.info("Starting heartbeat data consumer")
    
    # Use logger for waiting functions
    wait_for_kafka(logger=logger)
    wait_for_postgres(logger=logger)
    
    consumer = create_kafka_consumer()
    if not consumer:
        logger.critical("Failed to create Kafka consumer. Exiting.")
        return
    
    conn = create_db_connection()
    if not conn:
        logger.critical("Failed to connect to PostgreSQL. Exiting.")
        if consumer:
            consumer.close()
        return
    
    logger.info(f"Starting heart beat data consumer")
    logger.info(f"Consuming from topic: {KAFKA_TOPIC}")
    
    # Track metrics
    start_time = time.time()
    total_processed = 0
    
    try:
        while True:
            processed_count = process_messages(consumer, conn)
            total_processed += processed_count
            
            # Log statistics every 5 minutes
            current_time = time.time()
            if current_time - start_time > 300:  # 5 minutes
                elapsed_minutes = (current_time - start_time) / 60
                msg_per_minute = total_processed / elapsed_minutes if elapsed_minutes > 0 else 0
                logger.info(f"Performance: Processed {total_processed} messages in {elapsed_minutes:.1f} minutes ({msg_per_minute:.1f}/min)")
                # Reset counters
                start_time = current_time
                total_processed = 0
            
            if processed_count == 0:
                # If no messages were processed, sleep briefly to avoid busy waiting
                time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Stopping heart beat data consumer due to keyboard interrupt")
    except Exception as e:
        logger.critical(f"Unexpected error in consumer: {e}", exc_info=True)
    finally:
        # Close connections
        if consumer:
            try:
                consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        
        if conn:
            try:
                conn.close()
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")

if __name__ == "__main__":
    run_consumer()
