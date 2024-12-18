
  create or replace   view DEV_DB.DBT_VAULT.fct_source_directories
  
   as (
    -- Because we often work with multiple data sources, in our staging directory, we create one directory per source.

-- This model finds all cases where a source definition is NOT in the appropriate subdirectory.



with all_graph_resources as (
    select * from DEV_DB.DBT_VAULT.int_all_graph_resources
    where not is_excluded
),

-- find all sources that are definied in a .yml file NOT in their subdirectory
inappropriate_subdirectories_sources as (
    select 
        resource_name,
        resource_type,
        file_path as current_file_path,
        'models/' || 'staging' || '/' || source_name || '/' || file_name as change_file_path_to
    from all_graph_resources
    where resource_type = 'source'
    and directory_path not like '%' || source_name || '%'
)

select * from inappropriate_subdirectories_sources



    

    
    

    

    


  );

