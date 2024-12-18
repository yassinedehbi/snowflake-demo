
  create or replace   view DEV_DB.DBT_VAULT.v_stg_sales
  
   as (
    














-- Generated by AutomateDV (formerly known as dbtvault)

    

WITH source_data AS (

    SELECT

    PARTITION_KEY,
    SUBPARTITION_KEY,
    TICKET_CODE,
    TICKET_LINE_CODE,
    LOCATION_ID,
    LOCATION_ORIGINAL_ID,
    BUSINESS_DATE,
    REGISTER_CODE,
    TRANSACTION_TIME,
    TRANSACTION_TYPE_ID,
    TRANSACTION_TYPE_CODE,
    SKU_ID,
    SALE_ASSISTANT_ID,
    SALE_ASSISTANT_CODE,
    CUSTOMER_CODE,
    QTY,
    AMT_DISCOUNTED,
    AMT_DISCOUNTED_TAXED,
    AMT_TAX,
    AMT_DISCOUNT,
    AMT_DISCOUNT_TAXED,
    CURRENCY_ID,
    CLIENTELE_AREA_STATE_ID,
    CLIENTELE_AREA_REGIONAL_ID,
    CLIENTELE_AREA_WW_ID,
    MARK_DOWN_ID,
    LOCAL_MARK_DOWN_ID,
    SALE_MARK_DOWN_ID,
    TYPOLOGY_ID,
    TYPOLOGY_CODE,
    TICKET_LINE_TYPE,
    AUDIT_TYPE_ID,
    AUDIT_TYPE_CODE,
    TAG_SOURCE_CODE,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __INSERT_TSTAMP,
    __UPDATE_TSTAMP,
    __DELETE_FLAG,
    DATE_PARTITION,
    LDTS,
    SOURCE_FILE,
    TRANSACTION_SALES

    FROM DEV_DB.DBT_VAULT.raw_sales
),

derived_columns AS (

    SELECT

    PARTITION_KEY,
    SUBPARTITION_KEY,
    TICKET_CODE,
    TICKET_LINE_CODE,
    LOCATION_ID,
    LOCATION_ORIGINAL_ID,
    BUSINESS_DATE,
    REGISTER_CODE,
    TRANSACTION_TIME,
    TRANSACTION_TYPE_ID,
    TRANSACTION_TYPE_CODE,
    SKU_ID,
    SALE_ASSISTANT_ID,
    SALE_ASSISTANT_CODE,
    CUSTOMER_CODE,
    QTY,
    AMT_DISCOUNTED,
    AMT_DISCOUNTED_TAXED,
    AMT_TAX,
    AMT_DISCOUNT,
    AMT_DISCOUNT_TAXED,
    CURRENCY_ID,
    CLIENTELE_AREA_STATE_ID,
    CLIENTELE_AREA_REGIONAL_ID,
    CLIENTELE_AREA_WW_ID,
    MARK_DOWN_ID,
    LOCAL_MARK_DOWN_ID,
    SALE_MARK_DOWN_ID,
    TYPOLOGY_ID,
    TYPOLOGY_CODE,
    TICKET_LINE_TYPE,
    AUDIT_TYPE_ID,
    AUDIT_TYPE_CODE,
    TAG_SOURCE_CODE,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __INSERT_TSTAMP,
    __UPDATE_TSTAMP,
    __DELETE_FLAG,
    DATE_PARTITION,
    LDTS,
    SOURCE_FILE,
    TRANSACTION_SALES,
    SOURCE_FILE AS RECORD_SOURCE,
    TRANSACTION_SALES AS SALES_KEY,
    LDTS AS LOAD_DATE

    FROM source_data
),

hashed_columns AS (

    SELECT

    PARTITION_KEY,
    SUBPARTITION_KEY,
    TICKET_CODE,
    TICKET_LINE_CODE,
    LOCATION_ID,
    LOCATION_ORIGINAL_ID,
    BUSINESS_DATE,
    REGISTER_CODE,
    TRANSACTION_TIME,
    TRANSACTION_TYPE_ID,
    TRANSACTION_TYPE_CODE,
    SKU_ID,
    SALE_ASSISTANT_ID,
    SALE_ASSISTANT_CODE,
    CUSTOMER_CODE,
    QTY,
    AMT_DISCOUNTED,
    AMT_DISCOUNTED_TAXED,
    AMT_TAX,
    AMT_DISCOUNT,
    AMT_DISCOUNT_TAXED,
    CURRENCY_ID,
    CLIENTELE_AREA_STATE_ID,
    CLIENTELE_AREA_REGIONAL_ID,
    CLIENTELE_AREA_WW_ID,
    MARK_DOWN_ID,
    LOCAL_MARK_DOWN_ID,
    SALE_MARK_DOWN_ID,
    TYPOLOGY_ID,
    TYPOLOGY_CODE,
    TICKET_LINE_TYPE,
    AUDIT_TYPE_ID,
    AUDIT_TYPE_CODE,
    TAG_SOURCE_CODE,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __INSERT_TSTAMP,
    __UPDATE_TSTAMP,
    __DELETE_FLAG,
    DATE_PARTITION,
    LDTS,
    SOURCE_FILE,
    TRANSACTION_SALES,
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

    CAST(MD5_BINARY(CONCAT(
        IFNULL(NULLIF(UPPER(TRIM(CAST(__DELETE_FLAG AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(__INSERT_TSTAMP AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(__UPDATE_TSTAMP AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AMT_DISCOUNT AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AMT_DISCOUNT_TAXED AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AMT_DISCOUNTED AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AMT_DISCOUNTED_TAXED AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AMT_TAX AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AUDIT_TYPE_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(AUDIT_TYPE_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(BUSINESS_DATE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(CLIENTELE_AREA_REGIONAL_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(CLIENTELE_AREA_STATE_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(CLIENTELE_AREA_WW_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(CURRENCY_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(CUSTOMER_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_PARTITION AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(INSERT_DATE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(LOCAL_MARK_DOWN_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(LOCATION_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(LOCATION_ORIGINAL_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(MARK_DOWN_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(OPERATION_FLAG AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(PARTITION_KEY AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(QTY AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(REGISTER_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(SALE_ASSISTANT_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(SALE_ASSISTANT_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(SALE_MARK_DOWN_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(SKU_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(SUBPARTITION_KEY AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TAG_SOURCE_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TICKET_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TICKET_LINE_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TICKET_LINE_TYPE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TRANSACTION_TIME AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TRANSACTION_TYPE_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TRANSACTION_TYPE_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TYPOLOGY_CODE AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(TYPOLOGY_ID AS VARCHAR))), ''), '^^'), '||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(UPDATE_DATE AS VARCHAR))), ''), '^^')
    )) AS BINARY(16)) AS SALES_HASHDIFF

    FROM derived_columns
),

columns_to_select AS (

    SELECT

    PARTITION_KEY,
    SUBPARTITION_KEY,
    TICKET_CODE,
    TICKET_LINE_CODE,
    LOCATION_ID,
    LOCATION_ORIGINAL_ID,
    BUSINESS_DATE,
    REGISTER_CODE,
    TRANSACTION_TIME,
    TRANSACTION_TYPE_ID,
    TRANSACTION_TYPE_CODE,
    SKU_ID,
    SALE_ASSISTANT_ID,
    SALE_ASSISTANT_CODE,
    CUSTOMER_CODE,
    QTY,
    AMT_DISCOUNTED,
    AMT_DISCOUNTED_TAXED,
    AMT_TAX,
    AMT_DISCOUNT,
    AMT_DISCOUNT_TAXED,
    CURRENCY_ID,
    CLIENTELE_AREA_STATE_ID,
    CLIENTELE_AREA_REGIONAL_ID,
    CLIENTELE_AREA_WW_ID,
    MARK_DOWN_ID,
    LOCAL_MARK_DOWN_ID,
    SALE_MARK_DOWN_ID,
    TYPOLOGY_ID,
    TYPOLOGY_CODE,
    TICKET_LINE_TYPE,
    AUDIT_TYPE_ID,
    AUDIT_TYPE_CODE,
    TAG_SOURCE_CODE,
    OPERATION_FLAG,
    INSERT_DATE,
    UPDATE_DATE,
    __INSERT_TSTAMP,
    __UPDATE_TSTAMP,
    __DELETE_FLAG,
    DATE_PARTITION,
    LDTS,
    SOURCE_FILE,
    TRANSACTION_SALES,
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
  );

