BEGIN;

CREATE INDEX IF NOT EXISTS idx_products_name_btree ON products USING btree (name);
CREATE INDEX IF NOT EXISTS idx_products_name_id_btree ON products USING btree (name, id);
CREATE INDEX IF NOT EXISTS idx_products_category_name_btree ON products USING btree (category, name);

COMMIT;