SELECT *
FROM {{ ref('fact_sales') }}
WHERE total_amount <= 0.00
