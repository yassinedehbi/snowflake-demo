














-- Generated by AutomateDV (formerly known as dbtvault)

    

WITH source_data AS (

    SELECT *

    FROM DBT_DB.DBT_VAULT.raw_sales
),

derived_columns AS (

    SELECT

    SOURCE_FILE AS RECORD_SOURCE,
    TRANSACTION_SALES AS SALES_KEY,
    LDTS AS LOAD_DATE

    FROM source_data
),

hashed_columns AS (

    SELECT

    RECORD_SOURCE,
    SALES_KEY,
    LOAD_DATE,

    CAST(MD5_BINARY(NULLIF(UPPER(TRIM(CAST(TRANSACTION_SALES AS VARCHAR))), '')) AS BINARY(16)) AS SALES_HK,

    CAST(MD5_BINARY(NULLIF(UPPER(TRIM(CAST(LOCATION_ID AS VARCHAR))), '')) AS BINARY(16)) AS LOCATION_HK,

    CAST(MD5_BINARY(NULLIF(UPPER(TRIM(CAST(SKU_ID AS VARCHAR))), '')) AS BINARY(16)) AS PRODUCT_HK,

    CAST(MD5_BINARY(NULLIF(CONCAT(
        IFNULL(NULLIF(UPPER(TRIM(CAST(SKU_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TRANSACTION_SALES AS VARCHAR))), ''), '^^')
    ), '^^||^^')) AS BINARY(16)) AS PRODUCT_SALES_PK,

    CAST(MD5_BINARY(NULLIF(CONCAT(
        IFNULL(NULLIF(UPPER(TRIM(CAST(LOCATION_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TRANSACTION_SALES AS VARCHAR))), ''), '^^')
    ), '^^||^^')) AS BINARY(16)) AS LOCATION_SALES_PK,

    CAST(MD5_BINARY() AS BINARY(16)) AS SALES_HASHDIFF

    FROM derived_columns
),

columns_to_select AS (

    SELECT

    RECORD_SOURCE,
    SALES_KEY,
    LOAD_DATE,
    SALES_HK,
    LOCATION_HK,
    PRODUCT_HK,
    PRODUCT_SALES_PK,
    LOCATION_SALES_PK,
    SALES_HASHDIFF

    FROM hashed_columns
)

SELECT * FROM columns_to_select