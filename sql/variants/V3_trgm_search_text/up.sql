BEGIN;

-- search_text column is now in base schema, only create index
CREATE INDEX IF NOT EXISTS idx_products_search_text_trgm ON products USING gin (search_text gin_trgm_ops);

COMMIT;