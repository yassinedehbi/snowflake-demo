with

final as (

    

        (
            select
                cast('DBT_DB.DBT_VAULT.base_node_columns' as TEXT) as _dbt_source_relation,

                

            from DBT_DB.DBT_VAULT.base_node_columns

            
        )

        union all
        

        (
            select
                cast('DBT_DB.DBT_VAULT.base_source_columns' as TEXT) as _dbt_source_relation,

                

            from DBT_DB.DBT_VAULT.base_source_columns

            
        )

        
)

select * from final