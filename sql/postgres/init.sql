-- First create the user with proper privileges on postgres database
CREATE USER analytics WITH PASSWORD 'analytics';

-- Then create the database with the analytics user as owner
CREATE DATABASE flight_analytics WITH OWNER analytics;

-- Connect to the new database
\connect flight_analytics

-- Create tables for analytics
CREATE TABLE IF NOT EXISTS airline_metrics (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100),
    avg_base_fare DECIMAL(10, 2),
    avg_total_fare DECIMAL(10, 2),
    booking_count INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS route_metrics (
    id SERIAL PRIMARY KEY,
    source VARCHAR(10),
    source_name VARCHAR(100),
    destination VARCHAR(10),
    destination_name VARCHAR(255),
    avg_fare DECIMAL(10, 2),
    booking_count INT,
    route_rank INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS seasonal_metrics (
    id SERIAL PRIMARY KEY,
    seasonality VARCHAR(50),
    airline VARCHAR(100),
    avg_fare DECIMAL(10, 2),
    booking_count INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO analytics;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO analytics;
