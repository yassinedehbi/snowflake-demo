
    
    

select
    transaction_sales as unique_field,
    count(*) as n_records

from DBT_DB.data_mart.fct_sales
where transaction_sales is not null
group by transaction_sales
having count(*) > 1


