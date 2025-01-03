{%- set yaml_metadata -%}

source_model: 'raw_location'
derived_columns:
  RECORD_SOURCE: 'SOURCE_FILE'
  LOCATION_KEY: 'LOCATION_ID'
  LOAD_DATE: 'LDTS'
--   EFFECTIVE_FROM: 'TRANSACTION_DATE'

hashed_columns:
    LOCATION_HK: 'LOCATION_ID'

    LOCATION_HASHDIFF:
        is_hashdiff: true
        columns:
            - 'LOCATION_CODE'
            - 'CITY_ID'
            - 'STATE_ID'
            - 'COUNTRY_ID'
            - 'DISTRICT_ID'
            - 'ZONE_ID'
            - 'REGION_ID'
            - 'AREA_ID'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}


{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
