WITH product_hub_sat AS (
    SELECT
        h.sku_id,
        s.*
    FROM {{ ref("hub_product") }} h
    INNER JOIN {{ ref("sat_product") }} s
    ON h.product_hk = s.product_hk
    QUALIFY lead(s.load_date) OVER (
        PARTITION BY h.product_hk
        ORDER BY s.load_date
    ) IS NULL
),
star_product AS (
    SELECT
        SKU_ID,
        LOAD_DATE,
        CLASS_ID,
        COLOR_ID,
        CURR_STATUS_ID,
        FUNCTION_ID,
        LINE_ID,
        MODEL_ID,
        STYLE_COLOR_ID,
        STYLE_ID
    FROM product_hub_sat
)
SELECT * 
FROM star_product