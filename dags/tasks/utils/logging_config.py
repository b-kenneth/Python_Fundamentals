# dags/tasks/utils/logging_config.py
import logging
import os
from datetime import datetime

def configure_logging(task_name):
    """Configure logging for the task"""
    # Create logs directory if it doesn't exist
    log_dir = "/opt/airflow/logs/custom"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create a log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = f"{log_dir}/{task_name}_{timestamp}.log"
    
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
