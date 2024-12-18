
    
    

select
    LOCATION_ID as unique_field,
    count(*) as n_records

from TEST_DB.STAGING.STG_LOCATION_RAW
where LOCATION_ID is not null
group by LOCATION_ID
having count(*) > 1


