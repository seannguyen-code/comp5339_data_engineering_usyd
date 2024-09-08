{{ config(
    materialized = 'table',
    unique_key = ['customer_key'] -- add more keys
) }}

SELECT
FROM
    {{ ref('staging_transactions') }}
