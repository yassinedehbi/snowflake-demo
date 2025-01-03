{# with selected_sat_sales as (
    select
        .....
    from {{ ref('sat_sales') }} as ss


),

select 
test


from {{ ref('sat_sales') }} as ss
left join {{ ref('link_sales') }} as ls on ss.sales_hk = ls.sales_hk
left join {{ ref('hub_location') }} as hl on ls.location_hk = hl.location_hk
left join {{ ref('hub_product') }} as hp on ls.product_hk = hp.product_hk #}