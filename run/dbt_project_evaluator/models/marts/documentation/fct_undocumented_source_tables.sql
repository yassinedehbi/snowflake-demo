
  create or replace   view DEV_DB.DBT_VAULT.fct_undocumented_source_tables
  
   as (
    with

all_resources as (
    select * from DEV_DB.DBT_VAULT.int_all_graph_resources
    where not is_excluded

),

final as (

    select
        resource_name

    from all_resources
    where not is_described and resource_type = 'source'

)

select * from final



    

    
    

    

    


  );

