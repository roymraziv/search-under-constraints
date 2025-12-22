BEGIN;

DROP INDEX IF EXISTS idx_products_name_btree;
DROP INDEX IF EXISTS idx_products_name_id_btree;
DROP INDEX IF EXISTS idx_products_category_name_btree;

COMMIT;