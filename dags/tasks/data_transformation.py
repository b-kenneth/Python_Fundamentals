# dags/tasks/data_transformation.py
import pandas as pd
import numpy as np
from airflow.hooks.mysql_hook import MySqlHook
import logging

def transform_and_compute_kpis(mysql_conn_id, table_name, **kwargs):
    """
    Transform flight data and compute KPIs
    
    Args:
        mysql_conn_id: Airflow connection ID for MySQL
        table_name: MySQL table name containing validated data
    """
    try:
        # Connect to MySQL
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # Fetch validated data
        query = f"SELECT * FROM {table_name} WHERE Processed = 1"
        df = mysql_hook.get_pandas_df(query)
        
        logging.info(f"Fetched {len(df)} validated rows for transformation")
        
        if df.empty:
            logging.info("No data to transform")
            return True
        
        # 1. Calculate Total Fare if not already present
        if 'Total_Fare_BDT' in df.columns:
            # Verify if Total_Fare matches Base_Fare + Tax_Surcharge
            calc_total = df['Base_Fare_BDT'] + df['Tax_Surcharge_BDT']
            mismatch = (abs(df['Total_Fare_BDT'] - calc_total) > 0.01).sum()
            if mismatch > 0:
                logging.info(f"Found {mismatch} rows with mismatched Total_Fare, recalculating")
                df['Total_Fare_BDT'] = calc_total
        else:
            logging.info("Total_Fare_BDT not found, calculating it")
            df['Total_Fare_BDT'] = df['Base_Fare_BDT'] + df['Tax_Surcharge_BDT']
        
        # 2. Compute KPIs
        
        # KPI 1: Average Fare by Airline
        airline_metrics = compute_airline_metrics(df)
        
        # KPI 2: Seasonal Fare Variation 
        seasonal_metrics = compute_seasonal_metrics(df)
        
        # KPI 3: Most Popular Routes
        route_metrics = compute_route_metrics(df)
        
        # Store KPI results in MySQL for later loading to PostgreSQL
        store_kpi_results(mysql_hook, airline_metrics, seasonal_metrics, route_metrics)
        
        # Mark as transformed in the original table
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # Update transformation status if you want to track it separately
        # You could add a new column like 'Transformed' if needed
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("Data transformation and KPI computation completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error in transform_and_compute_kpis: {str(e)}")
        raise

def compute_airline_metrics(df):
    """Compute average fare and booking count by airline"""
    try:
        # Group by Airline and calculate metrics
        airline_metrics = df.groupby('Airline').agg(
            avg_base_fare=('Base_Fare_BDT', 'mean'),
            avg_total_fare=('Total_Fare_BDT', 'mean'),
            booking_count=('Airline', 'count')
        ).reset_index()
        
        # Round fare values to 2 decimal places
        airline_metrics['avg_base_fare'] = airline_metrics['avg_base_fare'].round(2)
        airline_metrics['avg_total_fare'] = airline_metrics['avg_total_fare'].round(2)
        
        logging.info(f"Computed metrics for {len(airline_metrics)} airlines")
        return airline_metrics
    except Exception as e:
        logging.error(f"Error computing airline metrics: {str(e)}")
        raise

def compute_seasonal_metrics(df):
    """Compute seasonal fare variation"""
    try:
        # Define peak seasons
        # Assuming 'Seasonality' column exists with values like 'Regular', 'Winter Holidays', etc.
        if 'Seasonality' not in df.columns:
            logging.warning("Seasonality column not found, cannot compute seasonal metrics")
            return pd.DataFrame()
        
        # Group by Seasonality and Airline to calculate metrics
        seasonal_metrics = df.groupby(['Seasonality', 'Airline']).agg(
            avg_fare=('Total_Fare_BDT', 'mean'),
            booking_count=('Airline', 'count')
        ).reset_index()
        
        # Round fare values
        seasonal_metrics['avg_fare'] = seasonal_metrics['avg_fare'].round(2)
        
        logging.info(f"Computed seasonal metrics for {len(seasonal_metrics)} season-airline combinations")
        return seasonal_metrics
    except Exception as e:
        logging.error(f"Error computing seasonal metrics: {str(e)}")
        raise

def compute_route_metrics(df):
    """Identify most popular routes by booking count"""
    try:
        # Group by Source and Destination
        route_metrics = df.groupby(['Source', 'Source_Name', 'Destination', 'Destination_Name']).agg(
            avg_fare=('Total_Fare_BDT', 'mean'),
            booking_count=('Source', 'count')
        ).reset_index()
        
        # Round fare values
        route_metrics['avg_fare'] = route_metrics['avg_fare'].round(2)
        
        # Sort by booking count to find most popular routes
        route_metrics = route_metrics.sort_values('booking_count', ascending=False)
        
        # Add route rank
        route_metrics['route_rank'] = range(1, len(route_metrics) + 1)
        
        logging.info(f"Computed route metrics for {len(route_metrics)} routes")
        return route_metrics
    except Exception as e:
        logging.error(f"Error computing route metrics: {str(e)}")
        raise

def store_kpi_results(mysql_hook, airline_metrics, seasonal_metrics, route_metrics):
    """Store KPI results in MySQL tables"""
    try:
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # Create KPI tables if they don't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airline_metrics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                airline VARCHAR(100),
                avg_base_fare DECIMAL(10, 2),
                avg_total_fare DECIMAL(10, 2),
                booking_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seasonal_metrics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                seasonality VARCHAR(50),
                airline VARCHAR(100),
                avg_fare DECIMAL(10, 2),
                booking_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS route_metrics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                source VARCHAR(10),
                source_name VARCHAR(100),
                destination VARCHAR(10),
                destination_name VARCHAR(255),
                avg_fare DECIMAL(10, 2),
                booking_count INT,
                route_rank INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert airline metrics
        if not airline_metrics.empty:
            for _, row in airline_metrics.iterrows():
                cursor.execute("""
                    INSERT INTO airline_metrics 
                    (airline, avg_base_fare, avg_total_fare, booking_count)
                    VALUES (%s, %s, %s, %s)
                """, (row['Airline'], row['avg_base_fare'], row['avg_total_fare'], row['booking_count']))
        
        # Insert seasonal metrics
        if not seasonal_metrics.empty:
            for _, row in seasonal_metrics.iterrows():
                cursor.execute("""
                    INSERT INTO seasonal_metrics 
                    (seasonality, airline, avg_fare, booking_count)
                    VALUES (%s, %s, %s, %s)
                """, (row['Seasonality'], row['Airline'], row['avg_fare'], row['booking_count']))
        
        # Insert route metrics
        if not route_metrics.empty:
            for _, row in route_metrics.iterrows():
                cursor.execute("""
                    INSERT INTO route_metrics 
                    (source, source_name, destination, destination_name, avg_fare, booking_count, route_rank)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (row['Source'], row['Source_Name'], row['Destination'], 
                     row['Destination_Name'], row['avg_fare'], row['booking_count'], row['route_rank']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info("KPI results stored in MySQL successfully")
    except Exception as e:
        logging.error(f"Error storing KPI results: {str(e)}")
        raise
