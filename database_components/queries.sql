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