-- Generated by AutomateDV (formerly known as dbtvault)

    

WITH source_data AS (
    SELECT a.PRODUCT_HK, a.PRODUCT_HASHDIFF, a.LOAD_DATE, a.RECORD_SOURCE
    FROM DBT_DB.DBT_VAULT.v_stg_product AS a
    WHERE a.PRODUCT_HK IS NOT NULL
),

records_to_insert AS (
    SELECT DISTINCT stage.PRODUCT_HK, stage.PRODUCT_HASHDIFF, stage.LOAD_DATE, stage.RECORD_SOURCE
    FROM source_data AS stage
)

SELECT * FROM records_to_insert