SELECT *
FROM products
WHERE (name, id) > (:last_name, :last_id)
ORDER BY name, id
LIMIT 25;