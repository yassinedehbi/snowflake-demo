with

models as (
    select * from DBT_DB.DBT_VAULT.int_all_graph_resources
    where resource_type = 'model'
    and not is_excluded
),

conversion as (
    select
        resource_id,
        case when is_described then 1 else 0 end as is_described_model,
        
            case when model_type = 'base' then 1.0 else NULL end as is_base_model,
            case when is_described and model_type = 'base' then 1.0 else 0 end as is_described_base_model,
        
            case when model_type = 'staging' then 1.0 else NULL end as is_staging_model,
            case when is_described and model_type = 'staging' then 1.0 else 0 end as is_described_staging_model,
        
            case when model_type = 'intermediate' then 1.0 else NULL end as is_intermediate_model,
            case when is_described and model_type = 'intermediate' then 1.0 else 0 end as is_described_intermediate_model,
        
            case when model_type = 'marts' then 1.0 else NULL end as is_marts_model,
            case when is_described and model_type = 'marts' then 1.0 else 0 end as is_described_marts_model,
        
            case when model_type = 'other' then 1.0 else NULL end as is_other_model,
            case when is_described and model_type = 'other' then 1.0 else 0 end as is_described_other_model
        

    from models
),

final as (
    select
        convert_timezone('UTC', current_timestamp()) as measured_at,
        cast(count(*) as integer) as total_models,
        cast(sum(is_described_model) as integer) as documented_models,
        round(sum(is_described_model) * 100.00 / count(*), 2) as documentation_coverage_pct,
        
            round(
                
    ( sum(is_described_base_model) * 100 ) / nullif( ( count(is_base_model) ), 0)

            , 2) as base_documentation_coverage_pct,
        
            round(
                
    ( sum(is_described_staging_model) * 100 ) / nullif( ( count(is_staging_model) ), 0)

            , 2) as staging_documentation_coverage_pct,
        
            round(
                
    ( sum(is_described_intermediate_model) * 100 ) / nullif( ( count(is_intermediate_model) ), 0)

            , 2) as intermediate_documentation_coverage_pct,
        
            round(
                
    ( sum(is_described_marts_model) * 100 ) / nullif( ( count(is_marts_model) ), 0)

            , 2) as marts_documentation_coverage_pct,
        
            round(
                
    ( sum(is_described_other_model) * 100 ) / nullif( ( count(is_other_model) ), 0)

            , 2) as other_documentation_coverage_pct
        

    from models
    left join conversion
    on models.resource_id = conversion.resource_id
)

select * from final