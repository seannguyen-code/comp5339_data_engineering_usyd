WITH resellers AS (
    SELECT
      {{ dbt_utils.generate_surrogate_key(['reseller_id']) }} AS reseller_key,
      reseller_id,
      reseller_name,
      commission_pct,
      ROW_NUMBER() OVER (PARTITION BY reseller_id ORDER BY loaded_timestamp DESC) AS rn
    FROM {{ source('import', 'resellers') }}
)

SELECT 
  *
FROM resellers
WHERE rn = 1
