
  create or replace   view DEV_DB.DBT_VAULT.int_all_dag_relationships
  
   as (
    -- creates a cte called all_relationships that will either use "with recursive" or loops depending on the DW


with recursive direct_relationships as (
    select
        *
    from DEV_DB.DBT_VAULT.int_direct_relationships
    where resource_type <> 'test'
),

-- should this be a fct_ model?

-- recursive CTE
-- one record for every resource and each of its downstream children (including itself)
all_relationships (
    parent_id,
    parent,
    parent_resource_type,
    parent_model_type,
    parent_materialized,
    parent_access,
    parent_is_public,
    parent_source_name,
    parent_file_path,
    parent_directory_path,
    parent_file_name,
    parent_is_excluded,
    child_id,
    child,
    child_resource_type,
    child_model_type,
    child_materialized,
    child_access,
    child_is_public,
    child_source_name,
    child_file_path,
    child_directory_path,
    child_file_name,
    child_is_excluded,
    distance,
    path,
    is_dependent_on_chain_of_views
) as (
    -- anchor
    select distinct
        resource_id as parent_id,
        resource_name as parent,
        resource_type as parent_resource_type,
        model_type as parent_model_type,
        materialized as parent_materialized,
        access as parent_access,
        is_public as parent_is_public,
        source_name as parent_source_name,
        file_path as parent_file_path,
        directory_path as parent_directory_path,
        file_name as parent_file_name,
        is_excluded as parent_is_excluded,
        resource_id as child_id,
        resource_name as child,
        resource_type as child_resource_type,
        model_type as child_model_type,
        materialized as child_materialized,
        access as child_access,
        is_public as child_is_public,
        source_name as child_source_name,
        file_path as child_file_path,
        directory_path as child_directory_path,
        file_name as child_file_name,
        is_excluded as child_is_excluded,
        0 as distance,
        array_construct( resource_name ) as path,
        cast(null as boolean) as is_dependent_on_chain_of_views

    from direct_relationships
    -- where direct_parent_id is null 

    union all

    -- recursive clause
    select
        all_relationships.parent_id as parent_id,
        all_relationships.parent as parent,
        all_relationships.parent_resource_type as parent_resource_type,
        all_relationships.parent_model_type as parent_model_type,
        all_relationships.parent_materialized as parent_materialized,
        all_relationships.parent_access as parent_access,
        all_relationships.parent_is_public as parent_is_public,
        all_relationships.parent_source_name as parent_source_name,
        all_relationships.parent_file_path as parent_file_path,
        all_relationships.parent_directory_path as parent_directory_path,
        all_relationships.parent_file_name as parent_file_name,
        all_relationships.parent_is_excluded as parent_is_excluded,
        direct_relationships.resource_id as child_id,
        direct_relationships.resource_name as child,
        direct_relationships.resource_type as child_resource_type,
        direct_relationships.model_type as child_model_type,
        direct_relationships.materialized as child_materialized,
        direct_relationships.access as child_access,
        direct_relationships.is_public as child_is_public,
        direct_relationships.source_name as child_source_name,
        direct_relationships.file_path as child_file_path,
        direct_relationships.directory_path as child_directory_path,
        direct_relationships.file_name as child_file_name,
        direct_relationships.is_excluded as child_is_excluded,
        all_relationships.distance+1 as distance,
        array_append(all_relationships.path, direct_relationships.resource_name) as path,
        case
            when
                all_relationships.child_materialized in ('view', 'ephemeral')
                and coalesce(all_relationships.is_dependent_on_chain_of_views, true)
                then true
            else false
        end as is_dependent_on_chain_of_views

    from direct_relationships
    inner join all_relationships
        on all_relationships.child_id = direct_relationships.direct_parent_id

    

)



select * from all_relationships
order by parent, distance
  );

