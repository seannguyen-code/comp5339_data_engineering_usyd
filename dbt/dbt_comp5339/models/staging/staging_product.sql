with products as (

SELECT
from {{ref('src_products')}} e
join {{ref('geography')}} g on g.cityname = e.product_city

)

select ---

from products

where rn = 1
