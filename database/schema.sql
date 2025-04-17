-- Create Products Table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    stock_quantity INT NOT NULL CHECK (stock_quantity >= 0),
    reorder_level INT NOT NULL CHECK (reorder_level >= 0)
);

-- Create Customers Table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    tier VARCHAR(10)
);

-- Create Orders Table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) CHECK (total_amount >= 0),
    notes VARCHAR(255),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

-- Create Order Details Table
CREATE TABLE order_details (
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- Create Inventory Logs Table
CREATE TABLE inventory_logs (
    log_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    change_amount INT NOT NULL,
    new_stock_quantity INT NOT NULL,
    log_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    change_type VARCHAR(50) CHECK (change_type IN ('order', 'replenish', 'adjustment')),
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);
