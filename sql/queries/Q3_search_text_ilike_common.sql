SELECT *
FROM products
WHERE search_text ILIKE %(pattern)s;