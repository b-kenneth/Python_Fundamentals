-- sql/mysql/init.sql
CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

CREATE TABLE IF NOT EXISTS flight_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Airline VARCHAR(100),
    Source VARCHAR(10),
    Source_Name VARCHAR(100),
    Destination VARCHAR(10),
    Destination_Name VARCHAR(255),
    Departure_Date_Time DATETIME,
    Arrival_Date_Time DATETIME,
    Duration_hrs FLOAT,
    Stopovers VARCHAR(50),
    Aircraft_Type VARCHAR(50),
    Class VARCHAR(20),
    Booking_Source VARCHAR(50),
    Base_Fare_BDT DECIMAL(10, 2),
    Tax_Surcharge_BDT DECIMAL(10, 2),
    Total_Fare_BDT DECIMAL(10, 2),
    Seasonality VARCHAR(50),
    Days_Before_Departure INT
);

-- Create a user for Airflow connection if not exists
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON flight_staging.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
