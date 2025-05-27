select * from {{ source('staging', 'STG_PRODUCT_RAW') }}
