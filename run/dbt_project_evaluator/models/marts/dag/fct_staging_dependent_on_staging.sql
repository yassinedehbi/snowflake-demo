
  
    

        create or replace transient table DEV_DB.DBT_VAULT.fct_staging_dependent_on_staging
         as
        (-- check for cases where models in the staging layer are dependent on each other
with direct_model_relationships as (
    select  
        *
    from DEV_DB.DBT_VAULT.int_all_dag_relationships
    where parent_resource_type in ('model', 'snapshot')
    and child_resource_type in ('model', 'snapshot')
    and not parent_is_excluded
    and not child_is_excluded
    and distance = 1
),

bending_connections as (
    select
        parent,
        parent_model_type,
        child,
        child_model_type
    from direct_model_relationships
    where parent_model_type = 'staging'
    and child_model_type = 'staging'
)

select * from bending_connections



    

    
    

    

    


        );
      
  