select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
     as unique_field,
    count(*) as n_records

from TEST_DB.STAGING.STG_LOCATION_RAW
where  is not null
group by 
having count(*) > 1



      
    ) dbt_internal_test