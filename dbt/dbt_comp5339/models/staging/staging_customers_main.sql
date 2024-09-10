{{
    config(
        materialized = 'incremental'
    )
}}

WITH 

{% if is_incremental() %}
latest_customer AS (
    SELECT max(loaded_timestamp) AS max_customer 
    FROM {{ this }}
),
{% endif %}

customers_main AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([ '0', 'customer_id']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    loaded_timestamp
  FROM {{ source('import', 'customers') }}
  {% if is_incremental() %}
  WHERE loaded_timestamp > (SELECT max_customer FROM latest_customer LIMIT 1)
  {% endif %}
)

SELECT
  *
FROM customers_main
