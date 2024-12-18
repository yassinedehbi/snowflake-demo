
  create or replace   view DEV_DB.DBT_VAULT.fct_chained_views_dependencies
  
   as (
    with all_relationships as (
    select  
        *
    from DEV_DB.DBT_VAULT.int_all_dag_relationships
    where distance <> 0
    and not parent_is_excluded
    and not child_is_excluded
),

final as (
    select
        parent,
        child, -- the model with potentially long run time / compilation time, improve performance by breaking the upstream chain of views
        distance,
        path
    from all_relationships
    where is_dependent_on_chain_of_views
    and child_resource_type = 'model'
    and distance > 5
)

select * from final



    

    
    

    

    



order by distance desc
  );

