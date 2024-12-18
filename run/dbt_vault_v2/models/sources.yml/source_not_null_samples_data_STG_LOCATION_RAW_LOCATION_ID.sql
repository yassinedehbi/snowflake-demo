select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select LOCATION_ID
from TEST_DB.STAGING.STG_LOCATION_RAW
where LOCATION_ID is null



      
    ) dbt_internal_test