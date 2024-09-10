-- This test ensures that total_amount in the sales fact table is not null.
SELECT *
FROM {{ ref('fact_sales') }}
WHERE total_amount IS NULL
