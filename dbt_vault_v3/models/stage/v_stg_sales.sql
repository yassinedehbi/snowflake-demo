{%- set yaml_metadata -%}

source_model: 'raw_sales'
derived_columns:
  RECORD_SOURCE: 'SOURCE_FILE'
  SALES_KEY: 'TRANSACTION_SALES'
  LOAD_DATE: 'LDTS'
--   EFFECTIVE_FROM: 'TRANSACTION_DATE'
hashed_columns:
    SALES_HK: 'TRANSACTION_SALES'

    LOCATION_HK: 'LOCATION_ID'
    PRODUCT_HK: 'SKU_ID'

    SALES_HASHDIFF:
        is_hashdiff: true
        columns:
            - 'AMT_TAX'
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
