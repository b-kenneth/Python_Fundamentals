import time
import socket
import os

def wait_for_port(host, port, timeout=30):
    """Wait until a port starts accepting TCP connections.
    Args:
        host: Host to wait for.
        port: Port to wait for.
        timeout: Time in seconds to wait before raising a TimeoutError.
    """
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                break
        except OSError:
            time.sleep(0.1)
            if time.time() - start_time >= timeout:
                raise TimeoutError(f'Waited too long for {host}:{port} to start accepting connections')

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    kafka_host = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(':')[0]
    kafka_port = int(os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(':')[1])
    print(f"Waiting for Kafka at {kafka_host}:{kafka_port}...")
    wait_for_port(kafka_host, kafka_port)
    print("Kafka is ready!")
    
def wait_for_postgres():
    """Wait for PostgreSQL to be ready"""
    pg_host = os.environ.get('PG_HOST', 'localhost')
    pg_port = int(os.environ.get('PG_PORT', '5432'))
    print(f"Waiting for PostgreSQL at {pg_host}:{pg_port}...")
    wait_for_port(pg_host, pg_port)
    print("PostgreSQL is ready!")
