{{ config(
    materialized = 'table',
    unique_key = ['customer_key', 'transaction_id']
) }}

SELECT
  t.transaction_id,
  t.customer_key,
  t.product_key,
  t.channel_key,
  t.total_amount,
  t.qty
FROM {{ ref('staging_transactions') }} t
