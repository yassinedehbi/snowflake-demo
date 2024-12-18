
  create or replace   view DEV_DB.DBT_VAULT.fct_test_coverage
  
   as (
    with

test_counts as (
    select * from DEV_DB.DBT_VAULT.int_model_test_summary
    where resource_type = 'model'
),

conversion as (
    select
        resource_name,
        cast(case when number_of_tests_on_model > 0 then 1 else 0 end as boolean) as is_tested_model,
        
            case when model_type = 'base' then 1.0 else NULL end as is_base_model,
            case when number_of_tests_on_model > 0 and model_type = 'base' then 1.0 else 0 end as is_tested_base_model,
        
            case when model_type = 'staging' then 1.0 else NULL end as is_staging_model,
            case when number_of_tests_on_model > 0 and model_type = 'staging' then 1.0 else 0 end as is_tested_staging_model,
        
            case when model_type = 'intermediate' then 1.0 else NULL end as is_intermediate_model,
            case when number_of_tests_on_model > 0 and model_type = 'intermediate' then 1.0 else 0 end as is_tested_intermediate_model,
        
            case when model_type = 'marts' then 1.0 else NULL end as is_marts_model,
            case when number_of_tests_on_model > 0 and model_type = 'marts' then 1.0 else 0 end as is_tested_marts_model,
        
            case when model_type = 'other' then 1.0 else NULL end as is_other_model,
            case when number_of_tests_on_model > 0 and model_type = 'other' then 1.0 else 0 end as is_tested_other_model
        

    from test_counts
),

final as (
    select
        convert_timezone('UTC', current_timestamp()) as measured_at,
        cast(count(*) as integer) as total_models,
        cast(sum(number_of_tests_on_model) as integer) as total_tests,
        sum(cast(is_tested_model as integer)) as tested_models,
        round(sum(cast(is_tested_model as integer)) * 100.0 / count(*), 2) as test_coverage_pct,
        
            round(
                
    ( sum(is_tested_base_model) * 100.0 ) / nullif( ( count(is_base_model) ), 0)

            , 2) as base_test_coverage_pct,
        
            round(
                
    ( sum(is_tested_staging_model) * 100.0 ) / nullif( ( count(is_staging_model) ), 0)

            , 2) as staging_test_coverage_pct,
        
            round(
                
    ( sum(is_tested_intermediate_model) * 100.0 ) / nullif( ( count(is_intermediate_model) ), 0)

            , 2) as intermediate_test_coverage_pct,
        
            round(
                
    ( sum(is_tested_marts_model) * 100.0 ) / nullif( ( count(is_marts_model) ), 0)

            , 2) as marts_test_coverage_pct,
        
            round(
                
    ( sum(is_tested_other_model) * 100.0 ) / nullif( ( count(is_other_model) ), 0)

            , 2) as other_test_coverage_pct,
        
        round(sum(number_of_tests_on_model) * 1.0000 / count(*), 4) as test_to_model_ratio

    from test_counts
    left join conversion
    on test_counts.resource_name = conversion.resource_name
)

select * from final
  );

