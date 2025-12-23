SELECT *
FROM products
WHERE search_text ILIKE '%organic%'
  AND category = 'Snacks';