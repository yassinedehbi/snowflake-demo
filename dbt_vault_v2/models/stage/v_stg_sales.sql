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

    PRODUCT_SALES_PK:
        - 'SKU_ID'
        - 'TRANSACTION_SALES'

    LOCATION_SALES_PK:
        - 'LOCATION_ID'
        - 'TRANSACTION_SALES'
    SALES_HASHDIFF:
        is_hashdiff: true
        columns: []
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{# Example: Dynamically fetch columns from the raw_SALES model #}
{% set dynamic_columns = get_filtered_columns(ref('raw_sales'), except=['TRANSACTION_SALES', 'LDTS', 'SOURCE_FILE' ,'RECORD_SOURCE', 'SALES_KEY', 'LOAD_DATE']) %}

{# Populate the columns in SALES_HASHDIFF dynamically #}
{% set _ = metadata_dict['hashed_columns']['SALES_HASHDIFF'].update({
    'columns': dynamic_columns
}) %}


{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}
