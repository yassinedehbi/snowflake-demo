with 

_base_exposure_relationships as (
    select * from DBT_DB.DBT_VAULT.base_exposure_relationships
),

final as (
    select 
        md5(cast(coalesce(cast(resource_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(direct_parent_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as unique_id, 
        *
    from _base_exposure_relationships
)

select distinct * from final