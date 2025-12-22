BEGIN;

DROP INDEX IF EXISTS idx_products_search_vector;

ALTER TABLE products DROP COLUMN search_vector;

COMMIT;