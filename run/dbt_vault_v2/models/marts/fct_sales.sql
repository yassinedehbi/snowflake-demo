
  
    

        create or replace transient table DEV_DB.data_mart.fct_sales
         as
        (with selected_sat_sales as (
    select *
    from DEV_DB.DBT_VAULT.sat_sales qualify
        lead(load_date)
            over (partition by sales_hk order by load_date asc)
        is null
),

selected_sat_location as (
    select *
    from DEV_DB.DBT_VAULT.sat_location qualify
        lead(load_date)
            over (partition by location_hk order by load_date asc)
        is null
),

selected_sat_product as (
    select *
    from DEV_DB.DBT_VAULT.sat_product qualify
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
    hl.location_id,
    sl.location_code,
    ss.location_original_id,
    ss.business_date,
    ss.register_code,
    ss.transaction_time,
    ss.transaction_type_id,
    ss.transaction_type_code,
    hp.sku_id,
    sp.material_code
from DEV_DB.DBT_VAULT.hub_sales as hs
left join selected_sat_sales as ss on hs.sales_hk = ss.sales_hk
left join DEV_DB.DBT_VAULT.link_location_sales as lls on hs.sales_hk = lls.sales_hk
left join DEV_DB.DBT_VAULT.hub_location as hl on lls.location_hk = hl.location_hk
left join selected_sat_location as sl on hl.location_hk = sl.location_hk
left join DEV_DB.DBT_VAULT.link_product_sales as lps on hs.sales_hk = lps.sales_hk
left join DEV_DB.DBT_VAULT.hub_product as hp on lps.product_hk = hp.product_hk
left join selected_sat_product as sp on hp.product_hk = sp.product_hk
        );
      
  