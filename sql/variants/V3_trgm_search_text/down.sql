BEGIN;

DROP INDEX IF EXISTS idx_products_search_text_trgm;

ALTER TABLE products DROP COLUMN search_text;

COMMIT;