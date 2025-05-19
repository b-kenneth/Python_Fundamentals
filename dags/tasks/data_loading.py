# dags/tasks/data_loading.py
import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
import logging
from datetime import datetime

def load_data_to_postgres(mysql_conn_id, postgres_conn_id, **kwargs):
    """
    Load transformed data and KPI metrics from MySQL to PostgreSQL
    
    Args:
        mysql_conn_id: Airflow connection ID for MySQL
        postgres_conn_id: Airflow connection ID for PostgreSQL
    """
    try:
        # Connect to MySQL (source)
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # Connect to PostgreSQL (target)
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        # Load airline metrics
        logging.info("Loading airline metrics to PostgreSQL")
        load_airline_metrics(mysql_hook, postgres_hook)
        
        # Load seasonal metrics
        logging.info("Loading seasonal metrics to PostgreSQL")
        load_seasonal_metrics(mysql_hook, postgres_hook)
        
        # Load route metrics
        logging.info("Loading route metrics to PostgreSQL")
        load_route_metrics(mysql_hook, postgres_hook)
        
        logging.info("Data loading to PostgreSQL completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error in load_data_to_postgres: {str(e)}")
        raise

def load_airline_metrics(mysql_hook, postgres_hook):
    """Load airline metrics from MySQL to PostgreSQL"""
    try:
        # Fetch airline metrics from MySQL
        query = """
            SELECT airline, avg_base_fare, avg_total_fare, booking_count
            FROM airline_metrics
        """
        df = mysql_hook.get_pandas_df(query)
        
        if df.empty:
            logging.info("No airline metrics to load")
            return
        
        logging.info(f"Fetched {len(df)} airline metrics records")
        
        # Prepare for PostgreSQL insert
        # First, truncate the target table to avoid duplication
        # Use a transaction to ensure consistency
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Start transaction
        cursor.execute("BEGIN;")
        
        # Truncate existing data - this approach replaces all data
        # For an incremental approach, you would need a different strategy
        cursor.execute("TRUNCATE TABLE airline_metrics;")
        
        # Insert new data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO airline_metrics 
                (airline, avg_base_fare, avg_total_fare, booking_count, updated_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['airline'], 
                row['avg_base_fare'], 
                row['avg_total_fare'], 
                row['booking_count'],
                datetime.now()
            ))
        
        # Commit transaction
        cursor.execute("COMMIT;")
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully loaded {len(df)} airline metrics records into PostgreSQL")
    except Exception as e:
        logging.error(f"Error loading airline metrics: {str(e)}")
        raise

def load_seasonal_metrics(mysql_hook, postgres_hook):
    """Load seasonal metrics from MySQL to PostgreSQL"""
    try:
        # Fetch seasonal metrics from MySQL
        query = """
            SELECT seasonality, airline, avg_fare, booking_count
            FROM seasonal_metrics
        """
        df = mysql_hook.get_pandas_df(query)
        
        if df.empty:
            logging.info("No seasonal metrics to load")
            return
        
        logging.info(f"Fetched {len(df)} seasonal metrics records")
        
        # Prepare for PostgreSQL insert
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Start transaction
        cursor.execute("BEGIN;")
        
        # Truncate existing data
        cursor.execute("TRUNCATE TABLE seasonal_metrics;")
        
        # Insert new data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO seasonal_metrics 
                (seasonality, airline, avg_fare, booking_count, updated_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['seasonality'], 
                row['airline'], 
                row['avg_fare'], 
                row['booking_count'],
                datetime.now()
            ))
        
        # Commit transaction
        cursor.execute("COMMIT;")
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully loaded {len(df)} seasonal metrics records into PostgreSQL")
    except Exception as e:
        logging.error(f"Error loading seasonal metrics: {str(e)}")
        raise

def load_route_metrics(mysql_hook, postgres_hook):
    """Load route metrics from MySQL to PostgreSQL"""
    try:
        # Fetch route metrics from MySQL
        query = """
            SELECT source, source_name, destination, destination_name, 
                   avg_fare, booking_count, route_rank
            FROM route_metrics
        """
        df = mysql_hook.get_pandas_df(query)
        
        if df.empty:
            logging.info("No route metrics to load")
            return
        
        logging.info(f"Fetched {len(df)} route metrics records")
        
        # Prepare for PostgreSQL insert
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Start transaction
        cursor.execute("BEGIN;")
        
        # Truncate existing data
        cursor.execute("TRUNCATE TABLE route_metrics;")
        
        # Insert new data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO route_metrics 
                (source, source_name, destination, destination_name, 
                 avg_fare, booking_count, route_rank, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['source'], 
                row['source_name'], 
                row['destination'], 
                row['destination_name'], 
                row['avg_fare'], 
                row['booking_count'],
                row['route_rank'],
                datetime.now()
            ))
        
        # Commit transaction
        cursor.execute("COMMIT;")
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully loaded {len(df)} route metrics records into PostgreSQL")
    except Exception as e:
        logging.error(f"Error loading route metrics: {str(e)}")
        raise
