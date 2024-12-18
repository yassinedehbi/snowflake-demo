
  
    

        create or replace transient table DEV_DB.DBT_VAULT.stg_node_relationships
         as
        (with 

_base_node_relationships as (
    select * from DEV_DB.DBT_VAULT.base_node_relationships
),

final as (
    select 
        md5(cast(coalesce(cast(resource_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(direct_parent_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as unique_id, 
        *
    from _base_node_relationships
)

-- we need distinct as the graph lists relationships multiple times if they are ref'd multiple times
select distinct * from final
        );
      
  