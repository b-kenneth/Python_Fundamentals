
SELECT * FROM products;

SELECT * FROM customers;

-- VERIFY CONSTRAINTS
-- This should fail due to negative stock_quantity
INSERT INTO products (product_name, category, price, stock_quantity, reorder_level)
VALUES ('Test Product', 'Test', 10.00, -5, 0);


-- CHECK RELATIONSHIPS
-- This should fail because customer_id 999 doesn't exist
INSERT INTO orders (customer_id, total_amount)
VALUES (999, 100.00);


-- PLACE ORDER
-- Multiple orders can be made by a customer
CALL place_order(
    1,  -- Customer ID
    '[{"product_id": 1, "quantity": 10}, {"product_id": 3, "quantity": 5}]'::JSONB
);



-- QUERIES FOR FUNCTIONS

-- Order Summary
SELECT * FROM fn_customer_order_summary(1);

-- Report on low stock
SELECT * FROM fn_low_stock_report();

-- Customer Spending Summary
SELECT * FROM fn_customer_spending_summary();

-- Checking customer spending tiers
-- With default thresholds
SELECT * FROM fn_customer_spending_tiers();

-- Or with custom thresholds
SELECT * FROM fn_customer_spending_tiers(1500, 750);




--Test for updated inventory logs
SELECT * FROM products WHERE product_id = 1;

-- Update stock quantity directly
-- UPDATE products
UPDATE products
SET stock_quantity = stock_quantity + 7
WHERE product_id = 1;

-- Examine the inventory log
SELECT *
FROM inventory_logs
WHERE product_id = 1
ORDER BY log_timestamp DESC
LIMIT 5;



-- Views

SELECT * FROM vw_order_summary ORDER BY order_date DESC;

SELECT * FROM vw_low_stock_products;

-- Retrieve Latest Orders (via View):
SELECT * FROM vw_order_summary ORDER BY order_date DESC LIMIT 10;

-- Retrieve All Products Needing Replenishment (via View):
SELECT * FROM vw_low_stock_products;

