{%- set yaml_metadata -%}

source_model: 'raw_product'
derived_columns:
  RECORD_SOURCE: 'SOURCE_FILE'
  PRODUCT_KEY: 'SKU_ID'
  LOAD_DATE: 'LDTS'
--   EFFECTIVE_FROM: 'TRANSACTION_DATE'
hashed_columns:
    PRODUCT_HK: 'SKU_ID'

    PRODUCT_HASHDIFF:
        is_hashdiff: true
        columns:
            - 'STYLE_COLOR_CODE'
            - 'MODEL_CODE'
            - 'COLOR_CODE'
            - 'SUBCLASS_CODE'
            - 'CLASS_CODE'
            - 'LINE_CODE'
            - 'DEPARTMENT_CODE'
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
