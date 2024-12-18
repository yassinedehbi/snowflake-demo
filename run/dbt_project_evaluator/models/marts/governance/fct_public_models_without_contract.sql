
  create or replace   view DEV_DB.DBT_VAULT.fct_public_models_without_contract
  
   as (
    with 

all_resources as (
    select * from DEV_DB.DBT_VAULT.int_all_graph_resources
    where not is_excluded
),

final as (

    select 
        resource_name,
        is_public,
        is_contract_enforced
        
    from all_resources
    where 
        is_public 
        and not is_contract_enforced
)

select * from final



    

    
    

    

    


  );

