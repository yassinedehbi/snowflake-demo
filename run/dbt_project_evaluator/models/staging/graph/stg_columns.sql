
  create or replace   view DEV_DB.DBT_VAULT.stg_columns
  
   as (
    with

final as (

    
    

        (
            select
                cast('DEV_DB.DBT_VAULT.base_node_columns' as TEXT) as _dbt_source_relation,

                
                    cast("NODE_UNIQUE_ID" as character varying(16777216)) as "NODE_UNIQUE_ID" ,
                    cast("NAME" as character varying(16777216)) as "NAME" ,
                    cast("DESCRIPTION" as character varying(16777216)) as "DESCRIPTION" ,
                    cast("DATA_TYPE" as character varying(16777216)) as "DATA_TYPE" ,
                    cast("CONSTRAINTS" as character varying(16777216)) as "CONSTRAINTS" ,
                    cast("HAS_NOT_NULL_CONSTRAINT" as BOOLEAN) as "HAS_NOT_NULL_CONSTRAINT" ,
                    cast("CONSTRAINTS_COUNT" as NUMBER(38,0)) as "CONSTRAINTS_COUNT" ,
                    cast("QUOTE" as character varying(16777216)) as "QUOTE" 

            from DEV_DB.DBT_VAULT.base_node_columns

            
        )

        union all
        

        (
            select
                cast('DEV_DB.DBT_VAULT.base_source_columns' as TEXT) as _dbt_source_relation,

                
                    cast("NODE_UNIQUE_ID" as character varying(16777216)) as "NODE_UNIQUE_ID" ,
                    cast("NAME" as character varying(16777216)) as "NAME" ,
                    cast("DESCRIPTION" as character varying(16777216)) as "DESCRIPTION" ,
                    cast("DATA_TYPE" as character varying(16777216)) as "DATA_TYPE" ,
                    cast("CONSTRAINTS" as character varying(16777216)) as "CONSTRAINTS" ,
                    cast("HAS_NOT_NULL_CONSTRAINT" as BOOLEAN) as "HAS_NOT_NULL_CONSTRAINT" ,
                    cast("CONSTRAINTS_COUNT" as NUMBER(38,0)) as "CONSTRAINTS_COUNT" ,
                    cast("QUOTE" as character varying(16777216)) as "QUOTE" 

            from DEV_DB.DBT_VAULT.base_source_columns

            
        )

        
)

select * from final
  );

