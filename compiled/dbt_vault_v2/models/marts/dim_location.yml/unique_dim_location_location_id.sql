
    
    

select
    location_id as unique_field,
    count(*) as n_records

from DBT_DB.data_mart.dim_location
where location_id is not null
group by location_id
having count(*) > 1


