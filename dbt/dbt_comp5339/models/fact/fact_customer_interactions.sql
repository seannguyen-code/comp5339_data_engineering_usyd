{{
    config(
        materialized = 'table',
        unique_key = ['customer_key', 'transaction_id']
    )
}}

SELECT
  t.transaction_id,
  c.customer_key,
  t.amount AS total_amount,
  t.qty,
  t.bought_date
FROM {{ ref('staging_transactions') }} t
JOIN {{ ref('dim_customer') }} c ON t.customer_id = c.customer_key
