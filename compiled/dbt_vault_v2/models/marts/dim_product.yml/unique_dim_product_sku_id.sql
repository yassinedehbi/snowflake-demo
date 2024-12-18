
    
    

select
    sku_id as unique_field,
    count(*) as n_records

from DBT_DB.data_mart.dim_product
where sku_id is not null
group by sku_id
having count(*) > 1


