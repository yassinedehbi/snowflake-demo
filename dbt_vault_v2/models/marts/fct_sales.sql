with selected_sat_sales as (
    select *
    from {{ ref('sat_sales') }} qualify
        lead(load_date)
            over (partition by sales_hk order by load_date asc)
        is null
),

selected_sat_location as (
    select *
    from {{ ref('sat_location') }} qualify
        lead(load_date)
            over (partition by location_hk order by load_date asc)
        is null
),

selected_sat_product as (
    select *
    from {{ ref('sat_product') }} qualify
        lead(load_date)
            over (partition by product_hk order by load_date asc)
        is null
)

select
    hs.transaction_sales,
    ss.partition_key,
    ss.subpartition_key,
    ss.ticket_code,
    ss.ticket_line_code,
    ss.location_id,
    ss.location_original_id,
    ss.business_date,
    ss.register_code,
    ss.transaction_time,
    ss.transaction_type_id,
    ss.transaction_type_code
from {{ ref('hub_sales') }} as hs
left join selected_sat_sales as ss on hs.sales_hk = ss.sales_hk
left join {{ ref('link_location_sales') }} as lls on hs.sales_hk = lls.sales_hk
left join {{ ref('hub_location') }} as hl on lls.location_hk = hl.location_hk
left join selected_sat_location as sl on hl.location_hk = sl.location_hk
left join {{ ref('link_product_sales') }} as lps on hs.sales_hk = lps.sales_hk
left join {{ ref('hub_product') }} as hp on lps.product_hk = hp.product_hk
left join selected_sat_product as sp on hp.product_hk = sp.product_hk
