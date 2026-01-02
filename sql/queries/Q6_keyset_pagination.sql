SELECT *
FROM products
WHERE (name, id) > (%(last_name)s, %(last_id)s)
ORDER BY name, id
LIMIT %(limit)s;