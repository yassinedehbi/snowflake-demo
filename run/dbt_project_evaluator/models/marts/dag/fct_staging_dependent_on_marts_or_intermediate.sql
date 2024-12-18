
  
    

        create or replace transient table DEV_DB.DBT_VAULT.fct_staging_dependent_on_marts_or_intermediate
         as
        (-- cases where a staging model depends on a marts/intermediate model
-- data should flow from raw -> staging -> intermediate -> marts
with direct_model_relationships as (
    select  
        *
    from DEV_DB.DBT_VAULT.int_all_dag_relationships
    where distance = 1
    and parent_resource_type = 'model'
    and child_resource_type = 'model'
    and not parent_is_excluded
    and not child_is_excluded
),
final as (
    select
        parent,
        parent_model_type,
        child,
        child_model_type
    from direct_model_relationships
    where child_model_type = 'staging'
    and parent_model_type in ('marts', 'intermediate')
)
select * from final



    

    
    

    

    


        );
      
  