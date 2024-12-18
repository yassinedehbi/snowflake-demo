select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select 
from TEST_DB.STAGING.STG_LOCATION_RAW
where  is null



      
    ) dbt_internal_test