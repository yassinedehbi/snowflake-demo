select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select transaction_sales
from DEV_DB.data_mart.fct_sales
where transaction_sales is null



      
    ) dbt_internal_test