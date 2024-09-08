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

trans_xml AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(
      [ 'reseller_id', 'transaction_id']
    ) }} AS customer_key,
    reseller_id,
    transaction_id,
    product_name,
    total_amount,
    no_purchased_postcards,
    date_bought,
    sales_channel,
    office_location,
    loaded_timestamp
  FROM
    {{ source(
      'preprocessed',
      'resellerxmlextracted'
    ) }}


  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_timestamp > (SELECT max_transaction from latest_transaction LIMIT 1)

  {% endif %}



)


SELECT
----
FROM
  trans_xml t
  JOIN {{ ref('dim_product') }}
  e
  ON t.product_name = e.product_name
  JOIN {{ ref('dim_channel') }} C
  ON t.sales_channel = C.channel_name
  JOIN {{ ref('dim_customer') }}
  cu
  ON t.customer_key = cu.customer_key
  JOIN {{ ref('dim_salesagent') }}
  s
  ON t.reseller_id = s.original_reseller_id