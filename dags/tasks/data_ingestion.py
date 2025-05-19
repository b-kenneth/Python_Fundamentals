
import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook
import logging
import os
from datetime import datetime

def validate_csv(csv_path):
    """Validate that the CSV file exists and is readable"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Check if file is readable
    try:
        df = pd.read_csv(csv_path, nrows=5)
        logging.info(f"CSV validation successful for: {csv_path}")
        logging.info(f"Columns found: {', '.join(df.columns.tolist())}")
        return True
    except Exception as e:
        logging.error(f"Error validating CSV: {e}")
        raise

def csv_to_mysql(csv_path, mysql_conn_id, table_name, **kwargs):
    """
    Load CSV data into MySQL staging table
    
    Args:
        csv_path: Path to the CSV file
        mysql_conn_id: Airflow connection ID for MySQL
        table_name: Target MySQL table name
    """
    try:
        # Validate CSV before processing
        validate_csv(csv_path)
        
        # Read CSV file
        logging.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Clean column names (replace spaces with underscores)
        df.columns = [col.replace(' ', '_').replace('&', '').replace('(', '').replace(')', '').replace('-', '_') for col in df.columns]

        df.rename(columns=lambda x: x.replace('__', '_'), inplace=True)

        
        # Convert date columns
        datetime_columns = ['Departure_Date_Time', 'Arrival_Date_Time']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
        # Log data summary
        logging.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")
        logging.info(f"Columns after cleaning: {', '.join(df.columns.tolist())}")
        
        # Connect to MySQL
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # Get column names for insert query
        columns = df.columns.tolist()
        
        # Create temporary CSV for bulk insert
        temp_csv_path = f"/tmp/flight_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        df.to_csv(temp_csv_path, index=False, header=False)
        
        # Bulk insert data
        logging.info(f"Inserting data into MySQL table: {table_name}")
        
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # Create a SQL string for bulk insert
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Insert data in chunks to avoid memory issues
        chunk_size = 1000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            values = [tuple(row) for row in chunk.values]
            
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            cursor.executemany(sql, values)
            
            conn.commit()
            logging.info(f"Inserted chunk {i//chunk_size + 1}: {len(chunk)} rows")
        
        cursor.close()
        conn.close()
        
        # Cleanup temp file
        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            
        logging.info(f"Successfully loaded {len(df)} rows into {table_name}")
        return True
    
    except Exception as e:
        logging.error(f"Error in csv_to_mysql: {str(e)}")
        raise
