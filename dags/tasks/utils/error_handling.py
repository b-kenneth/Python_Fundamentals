# dags/tasks/utils/error_handling.py
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
import logging
from datetime import datetime

def log_error(task_id, error_message, context=None):
    """
    Log error to a central error logging table
    
    Args:
        task_id: The ID of the task that failed
        error_message: The error message
        context: Airflow context (optional)
    """
    try:
        # Get MySQL connection
        mysql_conn_id = "mysql_conn"
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # Create error logging table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS error_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            task_id VARCHAR(255) NOT NULL,
            dag_id VARCHAR(255),
            execution_date DATETIME,
            error_message TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        mysql_hook.run(create_table_sql)
        
        # Insert error log
        dag_id = context.get('dag').dag_id if context and 'dag' in context else None
        execution_date = context.get('execution_date') if context else datetime.now()
        
        insert_sql = """
        INSERT INTO error_logs (task_id, dag_id, execution_date, error_message)
        VALUES (%s, %s, %s, %s)
        """
        mysql_hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, error_message))
        
        logging.error(f"Error in task {task_id}: {error_message}")
        
    except Exception as e:
        logging.error(f"Failed to log error to database: {str(e)}")
        logging.error(f"Original error in task {task_id}: {error_message}")
