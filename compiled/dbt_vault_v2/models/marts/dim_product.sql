WITH product_hub_sat AS (
    SELECT
        s.*,
        h.sku_id
    FROM DBT_DB.DBT_VAULT.hub_product AS h
    INNER JOIN DBT_DB.DBT_VAULT.sat_product AS s
        ON h.product_hk = s.product_hk
    QUALIFY lead(s.load_date) OVER (
        PARTITION BY h.product_hk
        ORDER BY s.load_date
    ) IS NULL
),

star_product AS (
    SELECT
        sku_id,
        load_date,
        class_id,
        color_id,
        curr_status_id,
        function_id,
        line_id,
        model_id,
        style_color_id,
        style_id
    FROM product_hub_sat
)

SELECT *
FROM star_product