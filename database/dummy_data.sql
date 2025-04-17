-- Insert sample products into `products` table
INSERT INTO products (product_name, category, price, stock_quantity, reorder_level)
VALUES
('Wireless Keyboard', 'Electronics', 49.99, 50, 10),
('Noise-Cancelling Headphones', 'Electronics', 199.99, 30, 5),
('Power Bank 10000mAh', 'Accessories', 24.99, 100, 20),
('Smartphone Case', 'Accessories', 9.99, 200, 50),
('Desk Lamp', 'Home & Office', 34.99, 75, 15);



-- Insert sample customers into `customers` table
INSERT INTO customers (customer_name, email, phone)
VALUES
('Alice Johnson', 'alice.johnson@example.com', '+12345678901'),
('Bob Smith', 'bob.smith@example.com', '+12345678902'),
('Charlie Brown', 'charlie.brown@example.com', '+12345678903'),
('Dana White', 'dana.white@example.com', '+12345678904'),
('Eve Davis', 'eve.davis@example.com', '+12345678905');


