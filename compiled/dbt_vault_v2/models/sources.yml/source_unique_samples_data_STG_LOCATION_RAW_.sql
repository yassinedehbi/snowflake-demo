
    
    

select
     as unique_field,
    count(*) as n_records

from TEST_DB.STAGING.STG_LOCATION_RAW
where  is not null
group by 
having count(*) > 1


