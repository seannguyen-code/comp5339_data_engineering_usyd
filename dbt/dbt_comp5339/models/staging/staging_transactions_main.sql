{{
    config(
        materialized='incremental'
    )
}}


WITH 

  {% if is_incremental() %}

latest_transaction as (

select max(loaded_timestamp) as max_transaction  from {{ this }}

),

{% endif %}

trans_main AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(
      [ '0', 'customer_id']
    ) }} AS customer_key,
    customer_id,
    transaction_id,
    product_id,
    amount,
    qty,
    channel_id,
    bought_date,
    loaded_timestamp
  FROM
    {{ source(
      'import',
      'transactions'
    ) }}

  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_timestamp > (SELECT max_transaction from latest_transaction LIMIT 1)

  {% endif %}

)

SELECT
---

FROM
  trans_main t
  JOIN {{ ref('dim_product') }} e
  ON t.product_id = e.product_key
  JOIN {{ ref('dim_channel') }} C
  ON t.channel_id = C.channel_key
  JOIN {{ ref('dim_customer') }}
  cu
  ON t.customer_key = cu.customer_key

