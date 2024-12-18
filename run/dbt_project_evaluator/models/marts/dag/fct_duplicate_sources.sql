
  
    

        create or replace transient table DEV_DB.DBT_VAULT.fct_duplicate_sources
         as
        (with sources as (
    select
        resource_name,
        case 
            -- if you're using databricks but not the unity catalog, database will be null
            when database is NULL then schema || '.' || identifier 
            else database || '.' || schema || '.' || identifier 
        end as source_db_location 
    from DEV_DB.DBT_VAULT.int_all_graph_resources
    where resource_type = 'source'
    and not is_excluded
    -- we order the CTE so that listagg returns values correctly sorted for some warehouses
    order by 1, 2
),

source_duplicates as (
    select
        source_db_location,
        
    listagg(
        resource_name,
        ', '
        )
        within group (order by resource_name) as source_names
    from sources
    group by source_db_location
    having count(*) > 1
)

select * from source_duplicates



    

    
    

    

    


        );
      
  