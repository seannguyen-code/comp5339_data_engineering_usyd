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

trans_xml AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['reseller_id', 'transaction_id']) }} AS customer_key,
    reseller_id,
    transaction_id,
    product_name,
    total_amount,
    no_purchased_postcards,
    date_bought,
    sales_channel,
    office_location,
    loaded_timestamp
  FROM {{ source('preprocessed', 'resellerxmlextracted') }}
  {% if is_incremental() %}
  WHERE loaded_timestamp > (SELECT max_transaction FROM latest_transaction LIMIT 1)
  {% endif %}
)

SELECT
  *
FROM trans_xml
