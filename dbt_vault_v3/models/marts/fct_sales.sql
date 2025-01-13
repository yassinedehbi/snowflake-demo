WITH flash_sales_link AS (
    SELECT
        SALES_HK AS f_sales_hk,
        PRODUCT_HK AS f_product_hk,
        LOCATION_HK AS f_location_hk,
        LOAD_DATE
    FROM {{ ref('link_sales') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SALES_HK
        ORDER BY LOAD_DATE DESC
    ) = 1
),
sales_sat AS (
    SELECT
        SALES_HK AS fs_sales_hk,
        QTY,
        AMT_DISCOUNTED,
        AMT_DISCOUNTED_TAXED,
        TICKET_LINE_TYPE,
        AMT_DISCOUNT,
        AMT_DISCOUNT_TAXED
    FROM {{ ref('sat_sales') }}
),
star_flash_sales AS (
    SELECT
        l.LOAD_DATE,
        l.f_product_hk,
        l.f_location_hk,
        s.QTY,
        s.AMT_DISCOUNTED,
        s.AMT_DISCOUNTED_TAXED,
        s.TICKET_LINE_TYPE,
        s.AMT_DISCOUNT,
        s.AMT_DISCOUNT_TAXED
    FROM flash_sales_link l
    JOIN sales_sat s
    ON l.f_sales_hk = s.fs_sales_hk
)
SELECT *
FROM star_flash_sales