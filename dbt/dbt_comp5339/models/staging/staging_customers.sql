WITH customers_main AS (
    SELECT
      customer_id,
      first_name,
      last_name,
      email,
      loaded_timestamp
    FROM {{ ref('src_customers') }}
),
customers_csv AS (
    SELECT
      SPLIT_PART(SPLIT_PART(imported_file, '_', 3), '.', 1)::INT AS reseller_id,
      first_name,
      last_name,
      email,
      loaded_timestamp
    FROM {{ ref('src_customers_csv') }}
),
customers_xml AS (
    SELECT
      first_name,
      last_name,
      email,
      loaded_timestamp
    FROM {{ source('preprocessed', 'resellerxmlextracted') }}
)

SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  {{ dbt_utils.generate_surrogate_key(['c.customer_id', 'cu.reseller_id']) }} AS customer_key
FROM customers c
LEFT JOIN {{ ref('staging_resellers') }} cu ON c.customer_id = cu.customer_id
