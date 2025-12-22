BEGIN;

ALTER TABLE products ADD COLUMN search_vector tsvector
GENERATE ALWAYS AS (
    to_tsvector('english', concat_ws(' ', name, brand, category, description))
) STORED;

CREATE INDEX IF NOT EXISTS idx_products_search_vector ON products USING gin (search_vector);

COMMIT;