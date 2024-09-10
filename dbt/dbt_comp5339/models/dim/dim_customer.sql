{{
  config(
    materialized = 'table',
    unique_key = 'customer_key'
  )
}}

SELECT
  customer_key,
  customer_id,
  first_name,
  last_name,
  email
FROM {{ ref('staging_customers_main') }}
