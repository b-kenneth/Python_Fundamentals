-- ===================================
-- CUSTOMER ORDER SUMMARIES
-- ===================================

CREATE OR REPLACE FUNCTION fn_customer_order_summary(p_customer_id INT)
RETURNS TABLE (
    order_id INT, 
    order_date TIMESTAMPTZ, 
    total_amount DECIMAL(10,2), 
    total_items BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.order_id,
        o.order_date,
        o.total_amount,
        COALESCE(SUM(od.quantity), 0) AS total_items
    FROM orders o
    LEFT JOIN order_details od ON o.order_id = od.order_id
    WHERE o.customer_id = p_customer_id
    GROUP BY o.order_id, o.order_date, o.total_amount
    ORDER BY o.order_date DESC;
END;
$$ LANGUAGE plpgsql;


-- ===================================
-- LOW STOCK PRODUCT REPORT
-- ===================================

CREATE OR REPLACE FUNCTION fn_low_stock_report()
RETURNS TABLE (
    product_id INT,
    product_name VARCHAR(255),
    stock_quantity INT,
    reorder_level INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.product_id,
        p.product_name,
        p.stock_quantity,
        p.reorder_level
    FROM products p
    WHERE p.stock_quantity < p.reorder_level
    ORDER BY p.stock_quantity ASC;
END;
$$ LANGUAGE plpgsql;


-- ===================================
-- CUSTOMER SPENDING SUMMARY
-- ===================================

CREATE OR REPLACE FUNCTION fn_customer_spending_summary()
RETURNS TABLE (
    customer_id INT,
    customer_name VARCHAR(255),
    total_spent DECIMAL(10,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.customer_id,
        c.customer_name,
        COALESCE(SUM(o.total_amount), 0) AS total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
    ORDER BY total_spent DESC;
END;
$$ LANGUAGE plpgsql;


-- ===================================
-- CUSTOMER SPENDING TIER
-- ===================================

CREATE OR REPLACE FUNCTION fn_customer_spending_tiers(
    p_gold_threshold DECIMAL = 1000,
    p_silver_threshold DECIMAL = 500
)
RETURNS TABLE (
    customer_id INT,
    customer_name VARCHAR(255),
    total_spent DECIMAL(10,2),
    spending_tier VARCHAR(10)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.customer_id,
        c.customer_name,
        COALESCE(SUM(o.total_amount), 0) AS total_spent,
        CASE
            WHEN COALESCE(SUM(o.total_amount), 0) >= p_gold_threshold THEN 'Gold'
            WHEN COALESCE(SUM(o.total_amount), 0) >= p_silver_threshold THEN 'Silver'
            ELSE 'Bronze'
        END AS spending_tier
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
    ORDER BY total_spent DESC;
END;
$$ LANGUAGE plpgsql;



-- ===================================
-- AUTOMATED STOCK REPLENISHMENT
-- ===================================

CREATE OR REPLACE FUNCTION auto_replenish_stock()
RETURNS TRIGGER AS $$
DECLARE
    v_replenish_amount INT := 100; -- Default amount to replenish, could be made dynamic
BEGIN
    -- Check if stock has fallen below reorder level
    IF NEW.stock_quantity < NEW.reorder_level THEN
        -- Update the stock (without triggering the other logging trigger again)
        NEW.stock_quantity := NEW.stock_quantity + v_replenish_amount;
        
        -- Log the replenishment directly
        INSERT INTO inventory_logs (
            product_id, 
            change_amount, 
            new_stock_quantity, 
            change_type
        ) VALUES (
            NEW.product_id,
            v_replenish_amount,
            NEW.stock_quantity,
            'replenish'
        );
        
        -- Optional: Log activity for monitoring
        RAISE NOTICE 'Auto-replenished product ID % with % units', NEW.product_id, v_replenish_amount;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;



-- ===================================
-- UPDATE INVENTORY LOGS
-- ===================================

CREATE OR REPLACE FUNCTION log_inventory_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Only log if the stock level actually changes
    IF NEW.stock_quantity <> OLD.stock_quantity THEN
        INSERT INTO inventory_logs (product_id, change_amount, new_stock_quantity, change_type)
        VALUES (
            NEW.product_id,
            NEW.stock_quantity - OLD.stock_quantity,
            NEW.stock_quantity,
            'adjustment'
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
