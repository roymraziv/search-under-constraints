SELECT *
FROM products
WHERE search_vector @@ to_tsquery('english', %(query)s);
