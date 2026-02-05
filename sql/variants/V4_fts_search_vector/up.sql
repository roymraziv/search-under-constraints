BEGIN;


CREATE INDEX IF NOT EXISTS idx_products_search_vector ON products USING gin (search_vector);

COMMIT;