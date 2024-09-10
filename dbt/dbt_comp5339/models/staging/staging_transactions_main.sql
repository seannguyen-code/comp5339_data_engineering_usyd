{{
    config(
        materialized = 'incremental'
    )
}}

WITH 

{% if is_incremental() %}
latest_transaction AS (
    SELECT max(loaded_timestamp) AS max_transaction  
    FROM {{ this }}
),
{% endif %}

trans_main AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([ '0', 'customer_id']) }} AS customer_key,
    customer_id,
    transaction_id,
    product_id,
    amount,
    qty,
    channel_id,
    bought_date,
    loaded_timestamp
  FROM {{ source('import', 'transactions') }}
  {% if is_incremental() %}
  WHERE loaded_timestamp > (SELECT max_transaction FROM latest_transaction LIMIT 1)
  {% endif %}
)

SELECT
  t.*,
  p.product_name,
  c.channel_name,
  cu.first_name AS customer_name
FROM trans_main t
JOIN {{ ref('dim_product') }} p ON t.product_id = p.product_key
JOIN {{ ref('dim_channel') }} c ON t.channel_id = c.channel_key
JOIN {{ ref('dim_customer') }} cu ON t.customer_key = cu.customer_key
