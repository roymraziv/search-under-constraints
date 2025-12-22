BEGIN;

ALTER TABLE products ADD COLUMN search_text TEXT
GENERATE ALWAYS AS (
    concat_ws(' ', name, brand, category, description)
) STORED;

CREATE INDEX IF NOT EXISTS idx_products_search_text_trgm ON products USING gin (search_text gin_trgm_ops);

COMMIT;