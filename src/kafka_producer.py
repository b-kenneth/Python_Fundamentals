import json
import time
import os
import random
from utils import setup_logging, wait_for_kafka
from kafka import KafkaProducer
from data_generator import create_customer_profiles, generate_heart_beat_data

os.makedirs('logs', exist_ok=True)

# Setup logging
logger = setup_logging('heartbeat-producer', log_file='logs/heartbeat_producer.log')

# Kafka configuration parameters
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = 'customer-heartbeats'

# Generator parameters
MIN_CUSTOMERS = 10
MAX_CUSTOMERS = 20
GENERATION_INTERVAL = 1  # seconds between batches

def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            linger_ms=100  # Batching messages for efficiency
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}", exc_info=True)
        return None

def produce_to_kafka(producer, records):
    """Send heart beat records to Kafka"""
    success_count = 0
    error_count = 0
    
    for record in records:
        try:
            # Use customer_id as key for partitioning
            future = producer.send(
                KAFKA_TOPIC, 
                key=record["customer_id"].encode('utf-8'),
                value=record
            )
            # Wait for the message to be delivered
            record_metadata = future.get(timeout=10)
            logger.debug(f"Record sent: {record} | "
                      f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}")
            success_count += 1
        except Exception as e:
            logger.error(f"Error producing to Kafka for customer {record.get('customer_id', 'unknown')}: {e}", exc_info=True)
            error_count += 1
    
    logger.info(f"Batch complete: {success_count} messages sent successfully, {error_count} errors")

def run_generator():
    """Main function to run the heart beat data generator"""
    logger.info("Starting heartbeat data generator")
    
    # Wait for Kafka using the same logger
    wait_for_kafka(logger=logger)
    
    producer = create_kafka_producer()
    if not producer:
        logger.critical("Failed to create Kafka producer. Exiting.")
        return
    
    # Generate random number of customers
    num_customers = random.randint(MIN_CUSTOMERS, MAX_CUSTOMERS)
    customers = create_customer_profiles(num_customers)
    
    logger.info(f"Starting heart beat data generation for {len(customers)} customers")
    logger.info(f"Sending to topic: {KAFKA_TOPIC}")
    
    # Occasionally add or remove customers to simulate real-world scenarios
    last_customer_update = time.time()
    total_messages_sent = 0
    start_time = time.time()
    
    try:
        while True:
            current_time = time.time()
            
            # Every 30 seconds, potentially add or remove customers
            if current_time - last_customer_update > 30:
                last_customer_update = current_time
                
                # 50% chance to add or remove customers
                if random.random() < 0.5:
                    # Add 1-3 new customers
                    new_customer_count = random.randint(1, 3)
                    new_customers = create_customer_profiles(new_customer_count)
                    customers.extend(new_customers)
                    logger.info(f"Added {new_customer_count} new customers. Total: {len(customers)}")
                else:
                    # Remove 1-2 customers if we have more than minimum
                    if len(customers) > MIN_CUSTOMERS:
                        remove_count = min(random.randint(1, 2), len(customers) - MIN_CUSTOMERS)
                        for _ in range(remove_count):
                            removed = customers.pop(random.randrange(len(customers)))
                            logger.info(f"Removed customer {removed['customer_id']}. Total: {len(customers)}")
            
            records = generate_heart_beat_data(customers)
            produce_to_kafka(producer, records)
            total_messages_sent += len(records)
            
            # Log performance statistics every 5 minutes
            if current_time - start_time > 300:  # 5 minutes in seconds
                messages_per_second = total_messages_sent / (current_time - start_time)
                logger.info(f"Performance stats: {messages_per_second:.2f} messages/second over the last {(current_time - start_time)/60:.1f} minutes")
                # Reset counters
                total_messages_sent = 0
                start_time = current_time
                
            time.sleep(GENERATION_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Stopping heart beat data generation due to keyboard interrupt")
    except Exception as e:
        logger.critical(f"Unexpected error in generator: {e}", exc_info=True)
    finally:
        try:
            if producer:
                producer.flush()
                producer.close()
                logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")

if __name__ == "__main__":
    run_generator()