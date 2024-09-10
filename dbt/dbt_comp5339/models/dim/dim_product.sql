{{
  config(
    materialized = 'table',
    unique_key = 'product_key'
  )
}}

SELECT
  product_key,
  product_id,
  product_name,
  price
FROM {{ ref('staging_product') }}
