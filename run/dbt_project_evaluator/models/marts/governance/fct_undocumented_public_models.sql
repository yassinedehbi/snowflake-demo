
  create or replace   view DEV_DB.DBT_VAULT.fct_undocumented_public_models
  
   as (
    with 

all_resources as (
    select * from DEV_DB.DBT_VAULT.int_all_graph_resources
    where not is_excluded
),

final as (
    select 
        resource_name,
        access, 
        is_described, 
        total_defined_columns,
        total_described_columns
    
    from all_resources
    where 
        is_public 
        and (
            -- no model level description
            not is_described
            -- not all columns defined have descriptions
            or total_described_columns < total_defined_columns
            -- no columns defined at all
            or total_defined_columns = 0
        )
)

select * from final



    

    
    

    

    


  );

