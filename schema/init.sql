-- Create heartbeat tracking table
CREATE TABLE IF NOT EXISTS customer_heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    reading_time TIMESTAMP NOT NULL,  -- When the heart rate was measured
    heart_rate INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- When the record was inserted
);

-- Create indexes for efficient querying and time-series analysis
CREATE INDEX IF NOT EXISTS idx_customer_id ON customer_heartbeats(customer_id);
CREATE INDEX IF NOT EXISTS idx_reading_time ON customer_heartbeats(reading_time);
CREATE INDEX IF NOT EXISTS idx_customer_reading_time ON customer_heartbeats(customer_id, reading_time);
