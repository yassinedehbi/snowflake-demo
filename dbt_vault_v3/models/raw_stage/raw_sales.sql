select * from {{ source('staging', 'STG_SALES_RAW') }}
