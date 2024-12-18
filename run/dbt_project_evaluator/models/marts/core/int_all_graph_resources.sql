
  
    

        create or replace transient table DEV_DB.DBT_VAULT.int_all_graph_resources
         as
        (-- one row for each resource in the graph



with unioned as (

    
    

        (
            select
                cast('DEV_DB.DBT_VAULT.stg_nodes' as TEXT) as _dbt_source_relation,

                
                    cast("UNIQUE_ID" as character varying(16777216)) as "UNIQUE_ID" ,
                    cast("NAME" as character varying(16777216)) as "NAME" ,
                    cast("RESOURCE_TYPE" as character varying(16777216)) as "RESOURCE_TYPE" ,
                    cast("FILE_PATH" as character varying(16777216)) as "FILE_PATH" ,
                    cast("IS_ENABLED" as BOOLEAN) as "IS_ENABLED" ,
                    cast("MATERIALIZED" as character varying(16777216)) as "MATERIALIZED" ,
                    cast("ON_SCHEMA_CHANGE" as character varying(16777216)) as "ON_SCHEMA_CHANGE" ,
                    cast("MODEL_GROUP" as character varying(16777216)) as "MODEL_GROUP" ,
                    cast("ACCESS" as character varying(16777216)) as "ACCESS" ,
                    cast("LATEST_VERSION" as character varying(16777216)) as "LATEST_VERSION" ,
                    cast("VERSION" as character varying(16777216)) as "VERSION" ,
                    cast("DEPRECATION_DATE" as character varying(16777216)) as "DEPRECATION_DATE" ,
                    cast("IS_CONTRACT_ENFORCED" as BOOLEAN) as "IS_CONTRACT_ENFORCED" ,
                    cast("TOTAL_DEFINED_COLUMNS" as NUMBER(38,0)) as "TOTAL_DEFINED_COLUMNS" ,
                    cast("TOTAL_DESCRIBED_COLUMNS" as NUMBER(38,0)) as "TOTAL_DESCRIBED_COLUMNS" ,
                    cast("DATABASE" as character varying(16777216)) as "DATABASE" ,
                    cast("SCHEMA" as character varying(16777216)) as "SCHEMA" ,
                    cast("PACKAGE_NAME" as character varying(16777216)) as "PACKAGE_NAME" ,
                    cast("ALIAS" as character varying(16777216)) as "ALIAS" ,
                    cast("IS_DESCRIBED" as BOOLEAN) as "IS_DESCRIBED" ,
                    cast("COLUMN_NAME" as character varying(16777216)) as "COLUMN_NAME" ,
                    cast("META" as character varying(16777216)) as "META" ,
                    cast("HARD_CODED_REFERENCES" as character varying(16777216)) as "HARD_CODED_REFERENCES" ,
                    cast("NUMBER_LINES" as NUMBER(38,0)) as "NUMBER_LINES" ,
                    cast("SQL_COMPLEXITY" as FLOAT) as "SQL_COMPLEXITY" ,
                    cast("MACRO_DEPENDENCIES" as character varying(16777216)) as "MACRO_DEPENDENCIES" ,
                    cast("IS_GENERIC_TEST" as BOOLEAN) as "IS_GENERIC_TEST" ,
                    cast("IS_EXCLUDED" as BOOLEAN) as "IS_EXCLUDED" ,
                    cast(null as character varying(16777216)) as "EXPOSURE_TYPE" ,
                    cast(null as character varying(16777216)) as "MATURITY" ,
                    cast(null as character varying(16777216)) as "URL" ,
                    cast(null as character varying(16777216)) as "OWNER_NAME" ,
                    cast(null as character varying(16777216)) as "OWNER_EMAIL" ,
                    cast(null as character varying(16777216)) as "METRIC_TYPE" ,
                    cast(null as character varying(16777216)) as "LABEL" ,
                    cast(null as character varying(16777216)) as "METRIC_FILTER" ,
                    cast(null as character varying(16777216)) as "METRIC_MEASURE" ,
                    cast(null as character varying(16777216)) as "METRIC_MEASURE_ALIAS" ,
                    cast(null as character varying(16777216)) as "NUMERATOR" ,
                    cast(null as character varying(16777216)) as "DENOMINATOR" ,
                    cast(null as character varying(16777216)) as "EXPR" ,
                    cast(null as character varying(16777216)) as "METRIC_WINDOW" ,
                    cast(null as character varying(16777216)) as "GRAIN_TO_DATE" ,
                    cast(null as character varying(16777216)) as "SOURCE_NAME" ,
                    cast(null as BOOLEAN) as "IS_SOURCE_DESCRIBED" ,
                    cast(null as character varying(16777216)) as "LOADED_AT_FIELD" ,
                    cast(null as BOOLEAN) as "IS_FRESHNESS_ENABLED" ,
                    cast(null as character varying(16777216)) as "LOADER" ,
                    cast(null as character varying(16777216)) as "IDENTIFIER" 

            from DEV_DB.DBT_VAULT.stg_nodes

            
        )

        union all
        

        (
            select
                cast('DEV_DB.DBT_VAULT.stg_exposures' as TEXT) as _dbt_source_relation,

                
                    cast("UNIQUE_ID" as character varying(16777216)) as "UNIQUE_ID" ,
                    cast("NAME" as character varying(16777216)) as "NAME" ,
                    cast("RESOURCE_TYPE" as character varying(16777216)) as "RESOURCE_TYPE" ,
                    cast("FILE_PATH" as character varying(16777216)) as "FILE_PATH" ,
                    cast(null as BOOLEAN) as "IS_ENABLED" ,
                    cast(null as character varying(16777216)) as "MATERIALIZED" ,
                    cast(null as character varying(16777216)) as "ON_SCHEMA_CHANGE" ,
                    cast(null as character varying(16777216)) as "MODEL_GROUP" ,
                    cast(null as character varying(16777216)) as "ACCESS" ,
                    cast(null as character varying(16777216)) as "LATEST_VERSION" ,
                    cast(null as character varying(16777216)) as "VERSION" ,
                    cast(null as character varying(16777216)) as "DEPRECATION_DATE" ,
                    cast(null as BOOLEAN) as "IS_CONTRACT_ENFORCED" ,
                    cast(null as NUMBER(38,0)) as "TOTAL_DEFINED_COLUMNS" ,
                    cast(null as NUMBER(38,0)) as "TOTAL_DESCRIBED_COLUMNS" ,
                    cast(null as character varying(16777216)) as "DATABASE" ,
                    cast(null as character varying(16777216)) as "SCHEMA" ,
                    cast("PACKAGE_NAME" as character varying(16777216)) as "PACKAGE_NAME" ,
                    cast(null as character varying(16777216)) as "ALIAS" ,
                    cast("IS_DESCRIBED" as BOOLEAN) as "IS_DESCRIBED" ,
                    cast(null as character varying(16777216)) as "COLUMN_NAME" ,
                    cast("META" as character varying(16777216)) as "META" ,
                    cast(null as character varying(16777216)) as "HARD_CODED_REFERENCES" ,
                    cast(null as NUMBER(38,0)) as "NUMBER_LINES" ,
                    cast(null as FLOAT) as "SQL_COMPLEXITY" ,
                    cast(null as character varying(16777216)) as "MACRO_DEPENDENCIES" ,
                    cast(null as BOOLEAN) as "IS_GENERIC_TEST" ,
                    cast(null as BOOLEAN) as "IS_EXCLUDED" ,
                    cast("EXPOSURE_TYPE" as character varying(16777216)) as "EXPOSURE_TYPE" ,
                    cast("MATURITY" as character varying(16777216)) as "MATURITY" ,
                    cast("URL" as character varying(16777216)) as "URL" ,
                    cast("OWNER_NAME" as character varying(16777216)) as "OWNER_NAME" ,
                    cast("OWNER_EMAIL" as character varying(16777216)) as "OWNER_EMAIL" ,
                    cast(null as character varying(16777216)) as "METRIC_TYPE" ,
                    cast(null as character varying(16777216)) as "LABEL" ,
                    cast(null as character varying(16777216)) as "METRIC_FILTER" ,
                    cast(null as character varying(16777216)) as "METRIC_MEASURE" ,
                    cast(null as character varying(16777216)) as "METRIC_MEASURE_ALIAS" ,
                    cast(null as character varying(16777216)) as "NUMERATOR" ,
                    cast(null as character varying(16777216)) as "DENOMINATOR" ,
                    cast(null as character varying(16777216)) as "EXPR" ,
                    cast(null as character varying(16777216)) as "METRIC_WINDOW" ,
                    cast(null as character varying(16777216)) as "GRAIN_TO_DATE" ,
                    cast(null as character varying(16777216)) as "SOURCE_NAME" ,
                    cast(null as BOOLEAN) as "IS_SOURCE_DESCRIBED" ,
                    cast(null as character varying(16777216)) as "LOADED_AT_FIELD" ,
                    cast(null as BOOLEAN) as "IS_FRESHNESS_ENABLED" ,
                    cast(null as character varying(16777216)) as "LOADER" ,
                    cast(null as character varying(16777216)) as "IDENTIFIER" 

            from DEV_DB.DBT_VAULT.stg_exposures

            
        )

        union all
        

        (
            select
                cast('DEV_DB.DBT_VAULT.stg_metrics' as TEXT) as _dbt_source_relation,

                
                    cast("UNIQUE_ID" as character varying(16777216)) as "UNIQUE_ID" ,
                    cast("NAME" as character varying(16777216)) as "NAME" ,
                    cast("RESOURCE_TYPE" as character varying(16777216)) as "RESOURCE_TYPE" ,
                    cast("FILE_PATH" as character varying(16777216)) as "FILE_PATH" ,
                    cast(null as BOOLEAN) as "IS_ENABLED" ,
                    cast(null as character varying(16777216)) as "MATERIALIZED" ,
                    cast(null as character varying(16777216)) as "ON_SCHEMA_CHANGE" ,
                    cast(null as character varying(16777216)) as "MODEL_GROUP" ,
                    cast(null as character varying(16777216)) as "ACCESS" ,
                    cast(null as character varying(16777216)) as "LATEST_VERSION" ,
                    cast(null as character varying(16777216)) as "VERSION" ,
                    cast(null as character varying(16777216)) as "DEPRECATION_DATE" ,
                    cast(null as BOOLEAN) as "IS_CONTRACT_ENFORCED" ,
                    cast(null as NUMBER(38,0)) as "TOTAL_DEFINED_COLUMNS" ,
                    cast(null as NUMBER(38,0)) as "TOTAL_DESCRIBED_COLUMNS" ,
                    cast(null as character varying(16777216)) as "DATABASE" ,
                    cast(null as character varying(16777216)) as "SCHEMA" ,
                    cast("PACKAGE_NAME" as character varying(16777216)) as "PACKAGE_NAME" ,
                    cast(null as character varying(16777216)) as "ALIAS" ,
                    cast("IS_DESCRIBED" as BOOLEAN) as "IS_DESCRIBED" ,
                    cast(null as character varying(16777216)) as "COLUMN_NAME" ,
                    cast("META" as character varying(16777216)) as "META" ,
                    cast(null as character varying(16777216)) as "HARD_CODED_REFERENCES" ,
                    cast(null as NUMBER(38,0)) as "NUMBER_LINES" ,
                    cast(null as FLOAT) as "SQL_COMPLEXITY" ,
                    cast(null as character varying(16777216)) as "MACRO_DEPENDENCIES" ,
                    cast(null as BOOLEAN) as "IS_GENERIC_TEST" ,
                    cast(null as BOOLEAN) as "IS_EXCLUDED" ,
                    cast(null as character varying(16777216)) as "EXPOSURE_TYPE" ,
                    cast(null as character varying(16777216)) as "MATURITY" ,
                    cast(null as character varying(16777216)) as "URL" ,
                    cast(null as character varying(16777216)) as "OWNER_NAME" ,
                    cast(null as character varying(16777216)) as "OWNER_EMAIL" ,
                    cast("METRIC_TYPE" as character varying(16777216)) as "METRIC_TYPE" ,
                    cast("LABEL" as character varying(16777216)) as "LABEL" ,
                    cast("METRIC_FILTER" as character varying(16777216)) as "METRIC_FILTER" ,
                    cast("METRIC_MEASURE" as character varying(16777216)) as "METRIC_MEASURE" ,
                    cast("METRIC_MEASURE_ALIAS" as character varying(16777216)) as "METRIC_MEASURE_ALIAS" ,
                    cast("NUMERATOR" as character varying(16777216)) as "NUMERATOR" ,
                    cast("DENOMINATOR" as character varying(16777216)) as "DENOMINATOR" ,
                    cast("EXPR" as character varying(16777216)) as "EXPR" ,
                    cast("METRIC_WINDOW" as character varying(16777216)) as "METRIC_WINDOW" ,
                    cast("GRAIN_TO_DATE" as character varying(16777216)) as "GRAIN_TO_DATE" ,
                    cast(null as character varying(16777216)) as "SOURCE_NAME" ,
                    cast(null as BOOLEAN) as "IS_SOURCE_DESCRIBED" ,
                    cast(null as character varying(16777216)) as "LOADED_AT_FIELD" ,
                    cast(null as BOOLEAN) as "IS_FRESHNESS_ENABLED" ,
                    cast(null as character varying(16777216)) as "LOADER" ,
                    cast(null as character varying(16777216)) as "IDENTIFIER" 

            from DEV_DB.DBT_VAULT.stg_metrics

            
        )

        union all
        

        (
            select
                cast('DEV_DB.DBT_VAULT.stg_sources' as TEXT) as _dbt_source_relation,

                
                    cast("UNIQUE_ID" as character varying(16777216)) as "UNIQUE_ID" ,
                    cast("NAME" as character varying(16777216)) as "NAME" ,
                    cast("RESOURCE_TYPE" as character varying(16777216)) as "RESOURCE_TYPE" ,
                    cast("FILE_PATH" as character varying(16777216)) as "FILE_PATH" ,
                    cast("IS_ENABLED" as BOOLEAN) as "IS_ENABLED" ,
                    cast(null as character varying(16777216)) as "MATERIALIZED" ,
                    cast(null as character varying(16777216)) as "ON_SCHEMA_CHANGE" ,
                    cast(null as character varying(16777216)) as "MODEL_GROUP" ,
                    cast(null as character varying(16777216)) as "ACCESS" ,
                    cast(null as character varying(16777216)) as "LATEST_VERSION" ,
                    cast(null as character varying(16777216)) as "VERSION" ,
                    cast(null as character varying(16777216)) as "DEPRECATION_DATE" ,
                    cast(null as BOOLEAN) as "IS_CONTRACT_ENFORCED" ,
                    cast(null as NUMBER(38,0)) as "TOTAL_DEFINED_COLUMNS" ,
                    cast(null as NUMBER(38,0)) as "TOTAL_DESCRIBED_COLUMNS" ,
                    cast("DATABASE" as character varying(16777216)) as "DATABASE" ,
                    cast("SCHEMA" as character varying(16777216)) as "SCHEMA" ,
                    cast("PACKAGE_NAME" as character varying(16777216)) as "PACKAGE_NAME" ,
                    cast("ALIAS" as character varying(16777216)) as "ALIAS" ,
                    cast("IS_DESCRIBED" as BOOLEAN) as "IS_DESCRIBED" ,
                    cast(null as character varying(16777216)) as "COLUMN_NAME" ,
                    cast("META" as character varying(16777216)) as "META" ,
                    cast(null as character varying(16777216)) as "HARD_CODED_REFERENCES" ,
                    cast(null as NUMBER(38,0)) as "NUMBER_LINES" ,
                    cast(null as FLOAT) as "SQL_COMPLEXITY" ,
                    cast(null as character varying(16777216)) as "MACRO_DEPENDENCIES" ,
                    cast(null as BOOLEAN) as "IS_GENERIC_TEST" ,
                    cast("IS_EXCLUDED" as BOOLEAN) as "IS_EXCLUDED" ,
                    cast(null as character varying(16777216)) as "EXPOSURE_TYPE" ,
                    cast(null as character varying(16777216)) as "MATURITY" ,
                    cast(null as character varying(16777216)) as "URL" ,
                    cast(null as character varying(16777216)) as "OWNER_NAME" ,
                    cast(null as character varying(16777216)) as "OWNER_EMAIL" ,
                    cast(null as character varying(16777216)) as "METRIC_TYPE" ,
                    cast(null as character varying(16777216)) as "LABEL" ,
                    cast(null as character varying(16777216)) as "METRIC_FILTER" ,
                    cast(null as character varying(16777216)) as "METRIC_MEASURE" ,
                    cast(null as character varying(16777216)) as "METRIC_MEASURE_ALIAS" ,
                    cast(null as character varying(16777216)) as "NUMERATOR" ,
                    cast(null as character varying(16777216)) as "DENOMINATOR" ,
                    cast(null as character varying(16777216)) as "EXPR" ,
                    cast(null as character varying(16777216)) as "METRIC_WINDOW" ,
                    cast(null as character varying(16777216)) as "GRAIN_TO_DATE" ,
                    cast("SOURCE_NAME" as character varying(16777216)) as "SOURCE_NAME" ,
                    cast("IS_SOURCE_DESCRIBED" as BOOLEAN) as "IS_SOURCE_DESCRIBED" ,
                    cast("LOADED_AT_FIELD" as character varying(16777216)) as "LOADED_AT_FIELD" ,
                    cast("IS_FRESHNESS_ENABLED" as BOOLEAN) as "IS_FRESHNESS_ENABLED" ,
                    cast("LOADER" as character varying(16777216)) as "LOADER" ,
                    cast("IDENTIFIER" as character varying(16777216)) as "IDENTIFIER" 

            from DEV_DB.DBT_VAULT.stg_sources

            
        )

        

),

naming_convention_prefixes as (
    select * from DEV_DB.DBT_VAULT.stg_naming_convention_prefixes
), 

naming_convention_folders as (
    select * from DEV_DB.DBT_VAULT.stg_naming_convention_folders
), 

unioned_with_calc as (
    select 
        *,
        case 
            when resource_type = 'source' then  source_name || '.' || name
            when coalesce(version, '') != '' then name || '.v' || version 
            else name 
        end as resource_name,
        case
            when resource_type = 'source' then null
            else 

    split_part(
        name,
        '_',
        1
        )

||'_' 
        end as prefix,
        
  

    replace(
        file_path,
        regexp_replace(file_path,'.*/',''),
        ''
    )



    
  
 as directory_path,
        regexp_replace(file_path,'.*/','') as file_name
    from unioned
    where coalesce(is_enabled, True) = True and package_name != 'dbt_project_evaluator'
), 

joined as (

    select
        unioned_with_calc.unique_id as resource_id, 
        unioned_with_calc.resource_name, 
        unioned_with_calc.prefix, 
        unioned_with_calc.resource_type, 
        unioned_with_calc.file_path, 
        unioned_with_calc.directory_path,
        unioned_with_calc.is_generic_test,
        unioned_with_calc.file_name,
        case 
            when unioned_with_calc.resource_type in ('test', 'source', 'metric', 'exposure', 'seed') then null
            else nullif(naming_convention_prefixes.model_type, '')
        end as model_type_prefix,
        case 
            when unioned_with_calc.resource_type in ('test', 'source', 'metric', 'exposure', 'seed') then null
            when 

    position(
        
  
    '/'
  
 || naming_convention_folders.folder_name_value || 
  
    '/'
  
 in unioned_with_calc.directory_path
    ) = 0 then null
            else naming_convention_folders.model_type 
        end as model_type_folder,
        

    position(
        
  
    '/'
  
 || naming_convention_folders.folder_name_value || 
  
    '/'
  
 in unioned_with_calc.directory_path
    ) as position_folder,  
        nullif(unioned_with_calc.column_name, '') as column_name,
        
        unioned_with_calc.macro_dependencies like '%macro.dbt_utils.test_unique_combination_of_columns%' and unioned_with_calc.resource_type = 'test' as is_test_unique_combination_of_columns,  
        
        unioned_with_calc.macro_dependencies like '%macro.dbt.test_unique%' and unioned_with_calc.resource_type = 'test' as is_test_unique,  
        
        unioned_with_calc.macro_dependencies like '%macro.dbt.test_not_null%' and unioned_with_calc.resource_type = 'test' as is_test_not_null,  
        
        unioned_with_calc.is_enabled, 
        unioned_with_calc.materialized, 
        unioned_with_calc.on_schema_change, 
        unioned_with_calc.database, 
        unioned_with_calc.schema, 
        unioned_with_calc.package_name, 
        unioned_with_calc.alias, 
        unioned_with_calc.is_described, 
        unioned_with_calc.model_group, 
        unioned_with_calc.access, 
        unioned_with_calc.access = 'public' as is_public, 
        unioned_with_calc.latest_version, 
        unioned_with_calc.version, 
        unioned_with_calc.deprecation_date, 
        unioned_with_calc.is_contract_enforced, 
        unioned_with_calc.total_defined_columns, 
        unioned_with_calc.total_described_columns, 
        unioned_with_calc.exposure_type, 
        unioned_with_calc.maturity, 
        unioned_with_calc.url, 
        unioned_with_calc.owner_name,
        unioned_with_calc.owner_email,
        unioned_with_calc.meta,
        unioned_with_calc.macro_dependencies,
        unioned_with_calc.metric_type, 
        unioned_with_calc.label, 
        unioned_with_calc.metric_filter,
        unioned_with_calc.metric_measure,
        unioned_with_calc.metric_measure_alias,
        unioned_with_calc.numerator,
        unioned_with_calc.denominator,
        unioned_with_calc.expr,
        unioned_with_calc.metric_window,
        unioned_with_calc.grain_to_date,
        unioned_with_calc.source_name, -- NULL for non-source resources
        unioned_with_calc.is_source_described, 
        unioned_with_calc.loaded_at_field, 
        unioned_with_calc.is_freshness_enabled, 
        unioned_with_calc.loader, 
        unioned_with_calc.identifier,
        unioned_with_calc.hard_coded_references, -- NULL for non-model resources
        unioned_with_calc.number_lines, -- NULL for non-model resources
        unioned_with_calc.sql_complexity, -- NULL for non-model resources
        unioned_with_calc.is_excluded -- NULL for metrics and exposures

    from unioned_with_calc
    left join naming_convention_prefixes
        on unioned_with_calc.prefix = naming_convention_prefixes.prefix_value

    cross join naming_convention_folders   

), 

calculate_model_type as (
    select 
        *, 
        case 
            when resource_type in ('test', 'source', 'metric', 'exposure', 'seed') then null
            -- by default we will define the model type based on its prefix in the case prefix and folder types are different
            else coalesce(model_type_prefix, model_type_folder, 'other') 
        end as model_type,
        row_number() over (partition by resource_id order by position_folder desc) as folder_name_rank
    from joined
),

final as (
    select
        *
    from calculate_model_type
    where folder_name_rank = 1
)

select 
    *
from final
        );
      
  