WITH

customers_main AS (

    SELECT
    
    FROM {{ref()}}

),

customers_csv  AS (

    SELECT  

    split_part(split_part(imported_file, '_', 3),'.',1)::int AS reseller_id, -- add more columns

    FROM {{ref()}}
)
,

customers_xml AS (


    SELECT

    FROM {{source('preprocessed','resellerxmlextracted')}}
), 

customers AS (


select from customers_csv --transaction_id from csv and xml are used as customer_id

union 

select from customers_xml

union

select 0 as reseller_id, from customers_main
)

select 

  {{ dbt_utils.generate_surrogate_key([
      'c.reseller_id',
      'customer_id']
  ) }} as customer_key,
 
--- add more columns

from customers c
left join {{ref()}} s on c.reseller_id = s.original_reseller_id