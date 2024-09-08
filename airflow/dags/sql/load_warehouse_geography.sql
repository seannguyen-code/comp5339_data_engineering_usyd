-- you can revise this using the csv file dbt/dbt_comp5339/seeds/geography.csv
with lookup as (
SELECT 'Moskva (Moscow)' as cityname, 'Russia' as countryname, 'Eastern Europe' as regionname UNION ALL
SELECT 'London', 'UK', 'Western Europe' UNION ALL
SELECT 'St Petersburg', 'Russia', 'Eastern Europe' UNION ALL

) 

INSERT INTO warehouse.dim_geography(cityname, countryname, regionname)

SELECT distinct city, countryname, regionname 
from import.products e
join lookup l on e.city = l.cityname

where city not in (select cityname from warehouse.dim_geography)
