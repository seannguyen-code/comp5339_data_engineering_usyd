{{ config(
    materialized = 'table'
) }}


SELECT * FROM {{ref('staging_transactions_main')}}

UNION ALL

SELECT * FROM {{ref('staging_transactions_resellers_csv')}}

UNION ALL

SELECT * FROM {{ref('staging_transactions_resellers_xml')}}