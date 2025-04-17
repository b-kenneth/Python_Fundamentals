-- ===================================
-- ORDER PLACEMENT
-- ===================================

CREATE OR REPLACE PROCEDURE place_order(
    p_customer_id INT,
    product_quantities JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id INT;
    item JSONB;
    v_total_items INT := 0;
    v_discount_rate DECIMAL(5,2) := 0.00;
    v_original_total DECIMAL(10,2);
    v_discount_value DECIMAL(10,2);
    v_final_total DECIMAL(10,2);
BEGIN
    -- Create the order
    INSERT INTO orders (customer_id, total_amount)
    VALUES (p_customer_id, 0)
    RETURNING order_id INTO v_order_id;

    -- Process each product in the order
    FOR item IN SELECT * FROM jsonb_array_elements(product_quantities)
    LOOP
        -- Add to total items count
        v_total_items := v_total_items + (item->>'quantity')::INT;
        
        INSERT INTO order_details (order_id, product_id, quantity, price)
        SELECT 
            v_order_id, 
            (item->>'product_id')::INT, 
            (item->>'quantity')::INT, 
            price 
        FROM products 
        WHERE product_id = (item->>'product_id')::INT;

        UPDATE products
        SET stock_quantity = stock_quantity - (item->>'quantity')::INT
        WHERE product_id = (item->>'product_id')::INT;

        INSERT INTO inventory_logs (product_id, change_amount, new_stock_quantity, change_type)
        SELECT 
            (item->>'product_id')::INT,
            -(item->>'quantity')::INT,
            stock_quantity,
            'order'
        FROM products 
        WHERE product_id = (item->>'product_id')::INT;
    END LOOP;

    -- Calculate the original total
    SELECT SUM(quantity * price) INTO v_original_total
    FROM order_details
    WHERE order_id = v_order_id;
    
    -- Apply bulk discount logic
    IF v_total_items >= 10 THEN
        v_discount_rate := 0.10; -- 10% discount
    END IF;
    
    -- Calculate discount and final amounts
    v_discount_value := v_original_total * v_discount_rate;
    v_final_total := v_original_total - v_discount_value;
    
    -- Update order with final total and add a note about the discount if applicable
    UPDATE orders
    SET total_amount = v_final_total,
        notes = CASE 
                   WHEN v_discount_rate > 0 THEN 
                      'Bulk Discount: ' || (v_discount_rate * 100) || '% off. Original: $' || 
                      v_original_total || ', Discount: $' || v_discount_value
                   ELSE NULL
                END
    WHERE order_id = v_order_id;
END;
$$;
