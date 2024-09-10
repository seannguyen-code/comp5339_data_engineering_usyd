{{
    config(
        materialized = 'table',
        unique_key = ['product_key', 'transaction_id']
    )
}}

SELECT
  t.transaction_id,
  p.product_key,
  t.amount AS total_amount,
  t.qty,
  t.bought_date
FROM {{ ref('staging_transactions') }} t
JOIN {{ ref('dim_product') }} p ON t.product_id = p.product_key
