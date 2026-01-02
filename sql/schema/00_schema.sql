BEGIN;

DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    brand text NOT NULL,
    category text NOT NULL,
    description text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    search_text text GENERATED ALWAYS AS (
        COALESCE(name, '') || ' ' || 
        COALESCE(brand, '') || ' ' || 
        COALESCE(category, '') || ' ' || 
        COALESCE(description, '')
    ) STORED
);

COMMIT;