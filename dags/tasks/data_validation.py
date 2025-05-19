# dags/tasks/data_validation.py
import pandas as pd
import numpy as np
from airflow.hooks.mysql_hook import MySqlHook
import logging

def validate_required_columns(df, required_columns):
    """Check if all required columns exist in the dataframe"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    return True

def handle_missing_values(df):
    """Handle missing or null values in the dataframe"""
    # Count missing values before handling
    missing_before = df.isna().sum().sum()
    logging.info(f"Found {missing_before} missing values before handling")
    
    # Handle missing values for different column types
    # For numeric columns - replace with mean or 0
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        if df[col].isna().any():
            if 'Fare' in col:
                # For fare columns, use mean value
                df[col] = df[col].fillna(df[col].mean())
            else:
                # For other numeric columns, use 0
                df[col] = df[col].fillna(0)
    
    # For categorical/string columns - replace with 'Unknown'
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        df[col] = df[col].fillna('Unknown')
    
    # Count missing values after handling
    missing_after = df.isna().sum().sum()
    logging.info(f"Remaining missing values after handling: {missing_after}")
    
    return df

def validate_data_types(df):
    """Validate data types of the dataframe"""
    # Validate fare columns are numeric
    fare_columns = [col for col in df.columns if 'Fare' in col]
    for col in fare_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            try:
                # Try to convert to numeric
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logging.info(f"Converted {col} to numeric type")
            except Exception as e:
                logging.error(f"Could not convert {col} to numeric: {str(e)}")
    
    # Validate categorical columns have non-empty strings
    categorical_cols = ['Airline', 'Source', 'Destination', 'Class', 'Booking_Source', 'Seasonality']
    for col in categorical_cols:
        if col in df.columns:
            # Replace empty strings with 'Unknown'
            df[col] = df[col].replace('', 'Unknown')
            # Count number of 'Unknown' values
            unknown_count = (df[col] == 'Unknown').sum()
            if unknown_count > 0:
                logging.info(f"Found {unknown_count} 'Unknown' values in {col}")
    
    return df

def correct_inconsistencies(df):
    """Flag or correct inconsistencies in the dataframe"""
    # Check for negative fares
    fare_columns = [col for col in df.columns if 'Fare' in col]
    for col in fare_columns:
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            logging.warning(f"Found {neg_count} negative values in {col}")
            # Replace negative fares with absolute values
            df[col] = df[col].abs()
            logging.info(f"Converted negative values in {col} to positive")
    
    # Validate Total_Fare_BDT = Base_Fare_BDT + Tax_Surcharge_BDT
    if all(col in df.columns for col in ['Total_Fare_BDT', 'Base_Fare_BDT', 'Tax_Surcharge_BDT']):
        # Calculate expected total
        expected_total = df['Base_Fare_BDT'] + df['Tax_Surcharge_BDT']
        # Find rows where total doesn't match
        mismatched = (abs(df['Total_Fare_BDT'] - expected_total) > 0.01).sum()
        if mismatched > 0:
            logging.warning(f"Found {mismatched} rows where Total_Fare doesn't match Base_Fare + Tax_Surcharge")
            # Correct the total fare
            df['Total_Fare_BDT'] = expected_total
            logging.info("Corrected Total_Fare values")
    
    return df

def validate_flight_data(mysql_conn_id, table_name, **kwargs):
    """
    Validate flight data in MySQL staging table
    
    Args:
        mysql_conn_id: Airflow connection ID for MySQL
        table_name: MySQL table name containing the data to validate
    """
    try:
        # Connect to MySQL
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # Fetch data from MySQL
        query = f"SELECT * FROM {table_name} WHERE Processed = 0"
        df = mysql_hook.get_pandas_df(query)
        
        logging.info(f"Fetched {len(df)} unprocessed rows for validation")
        
        if df.empty:
            logging.info("No data to validate")
            return True
        
        # Required columns as per project requirements
        required_columns = ['Airline', 'Source', 'Destination', 'Base_Fare_BDT', 'Tax_Surcharge_BDT', 'Total_Fare_BDT']
        
        # Run validations
        validate_required_columns(df, required_columns)
        df = handle_missing_values(df)
        df = validate_data_types(df)
        df = correct_inconsistencies(df)
        
        # Save validated data back to MySQL
        # Create a temporary table for updates
        temp_table = f"{table_name}_validated"
        
        # Get connection and cursor
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # Create temporary table with same structure
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {temp_table} LIKE {table_name}")
        
        # Prepare data for insertion
        records = df.to_dict('records')
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Insert validated data into temp table
        insert_query = f"INSERT INTO {temp_table} ({columns_str}) VALUES ({placeholders})"
        cursor.executemany(insert_query, [tuple(record[col] for col in columns) for record in records])
        
        # Update processed flag in original table
        id_list = ', '.join([str(id) for id in df['id'].tolist()])
        cursor.execute(f"UPDATE {table_name} SET Processed = 1 WHERE id IN ({id_list})")
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Data validation completed successfully for {len(df)} rows")
        return True
        
    except Exception as e:
        logging.error(f"Error in validate_flight_data: {str(e)}")
        raise
