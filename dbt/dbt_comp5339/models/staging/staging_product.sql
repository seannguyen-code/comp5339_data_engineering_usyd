WITH products AS (
    SELECT
      product_id,
      product_name,
      cityname AS product_city,
      loaded_timestamp
    FROM {{ ref('src_products') }} e
    JOIN {{ ref('geography') }} g ON g.cityname = e.product_city
)

SELECT
  product_id,
  product_name,
  product_city
FROM products
