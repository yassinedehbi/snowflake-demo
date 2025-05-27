WITH flash_sales_link AS (
    SELECT
        sales_hk AS f_flash_sales_hk,
        product_hk AS f_product_hk,
        location_hk AS f_location_hk,
        load_date
    FROM {{ ref('link_sales') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sales_hk
        ORDER BY load_date DESC
    ) = 1
),

sales_sat AS (
    SELECT
        sales_hk AS fs_sales_hk,
        qty,
        amt_discounted,
        amt_discounted_taxed,
        ticket_line_type,
        amt_discount,
        amt_discount_taxed
    FROM {{ ref('sat_sales') }}
),

star_flash_sales AS (
    SELECT
        ROW_NUMBER()   OVER (ORDER BY f_flash_sales_hk) AS flash_sales_id,
        l.load_date,
        l.f_product_hk,
        l.f_location_hk,
        s.qty,
        s.amt_discounted,
        s.amt_discounted_taxed,
        s.ticket_line_type,
        s.amt_discount,
        s.amt_discount_taxed
    FROM flash_sales_link AS l
    LEFT JOIN sales_sat AS s
        ON l.f_flash_sales_hk = s.fs_sales_hk
)

SELECT *
FROM star_flash_sales
