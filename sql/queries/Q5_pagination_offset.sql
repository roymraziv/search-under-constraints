SELECT *
FROM products
ORDER BY name
OFFSET %(offset)s
LIMIT %(limit)s;