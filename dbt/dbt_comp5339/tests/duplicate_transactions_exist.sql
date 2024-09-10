-- This test ensures that no duplicate transactions exist in the fact table.
SELECT transaction_id, COUNT(*)
FROM {{ ref('fact_sales') }}
GROUP BY transaction_id
HAVING COUNT(*) > 1
