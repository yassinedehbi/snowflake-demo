select * from {{ source('samples_data', 'STG_SALES_RAW') }}
