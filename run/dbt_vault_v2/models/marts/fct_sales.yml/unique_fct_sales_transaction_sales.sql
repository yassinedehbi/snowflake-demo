select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    transaction_sales as unique_field,
    count(*) as n_records

from DEV_DB.data_mart.fct_sales
where transaction_sales is not null
group by transaction_sales
having count(*) > 1



      
    ) dbt_internal_test