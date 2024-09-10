{{
  config(
    materialized = 'table',
    unique_key = 'reseller_key'
  )
}}

SELECT
  reseller_key,
  reseller_id,
  reseller_name,
  commission_pct
FROM {{ ref('staging_resellers') }}
