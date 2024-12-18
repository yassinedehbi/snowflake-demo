


    

/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select 

    cast(null as TEXT) as unique_id,
    cast(null as TEXT) as name,
    cast(null as TEXT) as resource_type,
    cast(null as TEXT) as file_path,
    cast(True as boolean) as is_described,
    cast(null as TEXT) as metric_type,
    cast(null as TEXT) as label,
    cast(null as TEXT) as package_name,
    cast(null as TEXT) as metric_filter,
    cast(null as TEXT) as metric_measure,
    cast(null as TEXT) as metric_measure_alias,
    cast(null as TEXT) as numerator,
    cast(null as TEXT) as denominator,
    cast(null as TEXT) as expr,
    cast(null as TEXT) as metric_window,
    cast(null as TEXT) as grain_to_date,
    cast(null as TEXT) as meta

from dummy_cte
where false