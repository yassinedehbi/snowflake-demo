select distinct * from {{ source('staging', 'STG_LOCATION_RAW') }}
