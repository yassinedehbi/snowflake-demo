with

all_resources as (
    select * from DBT_DB.DBT_VAULT.int_all_graph_resources
    where not is_excluded

),

final as (

    select distinct
        resource_name

    from all_resources
    where not is_freshness_enabled and resource_type = 'source'

)

select * from final



    

    
    

    

    

