CREATE TABLE IF NOT EXISTS events (
    event_id SERIAL PRIMARY KEY,
    user_id INT,
    session_id VARCHAR(64),
    actions VARCHAR(20),
    product_id INT,
    price NUMERIC(10,2),
    event_time TIMESTAMP,
    user_agent VARCHAR(256)
);
