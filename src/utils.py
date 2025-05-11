import time
import socket
import os
import logging

def setup_logging(logger_name, log_file=None, level=logging.INFO):
    """Set up logging configuration
    
    Args:
        logger_name: Name for the logger instance
        log_file: Optional file path for logging. If None, only console logging is used
        level: Logging level (default: logging.INFO)
        
    Returns:
        A configured logger instance
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create handlers list - always include console handler
    handlers = [logging.StreamHandler()]
    
    # Add file handler if log_file is specified
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    # Configure basic logging
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=handlers,
        force=True  # Ensure this configuration is applied
    )
    
    # Get and return a logger with the specified name
    return logging.getLogger(logger_name)

def wait_for_port(host, port, timeout=30, logger=None):
    """Wait until a port starts accepting TCP connections.
    Args:
        host: Host to wait for.
        port: Port to wait for.
        timeout: Time in seconds to wait before raising a TimeoutError.
        logger: Optional logger instance to use instead of print statements
    """
    start_time = time.time()
    log_msg = lambda msg: logger.info(msg) if logger else print(msg)
    
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                break
        except OSError:
            time.sleep(0.1)
            if time.time() - start_time >= timeout:
                error_msg = f'Waited too long for {host}:{port} to start accepting connections'
                if logger:
                    logger.error(error_msg)
                raise TimeoutError(error_msg)

def wait_for_kafka(logger=None):
    """Wait for Kafka to be ready
    
    Args:
        logger: Optional logger instance to use instead of print statements
    """
    kafka_host = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(':')[0]
    kafka_port = int(os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(':')[1])
    
    log_msg = lambda msg: logger.info(msg) if logger else print(msg)
    log_msg(f"Waiting for Kafka at {kafka_host}:{kafka_port}...")
    
    wait_for_port(kafka_host, kafka_port, logger=logger)
    log_msg("Kafka is ready!")
    
def wait_for_postgres(logger=None):
    """Wait for PostgreSQL to be ready
    
    Args:
        logger: Optional logger instance to use instead of print statements
    """
    pg_host = os.environ.get('PG_HOST', 'localhost')
    pg_port = int(os.environ.get('PG_PORT', '5432'))
    
    log_msg = lambda msg: logger.info(msg) if logger else print(msg)
    log_msg(f"Waiting for PostgreSQL at {pg_host}:{pg_port}...")
    
    wait_for_port(pg_host, pg_port, logger=logger)
    log_msg("PostgreSQL is ready!")