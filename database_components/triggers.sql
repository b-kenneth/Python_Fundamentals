-- ===================================
-- AUTOMATED STOCK REPLENISHMENT
-- ===================================

CREATE TRIGGER trg_auto_replenish
BEFORE UPDATE OF stock_quantity ON products
FOR EACH ROW
WHEN (NEW.stock_quantity < NEW.reorder_level)
EXECUTE FUNCTION auto_replenish_stock();



-- ===================================
-- UPDATE INVENTORY LOGS
-- ===================================

CREATE TRIGGER trg_inventory_log
AFTER UPDATE OF stock_quantity ON products
FOR EACH ROW
EXECUTE FUNCTION log_inventory_change();