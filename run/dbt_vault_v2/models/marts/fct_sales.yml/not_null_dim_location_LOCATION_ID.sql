select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select LOCATION_ID
from DEV_DB.data_mart.dim_location
where LOCATION_ID is null



      
    ) dbt_internal_test