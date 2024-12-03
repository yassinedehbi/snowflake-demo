{%- set source_model = "v_stg_product" -%}
{%- set src_pk = "PRODUCT_HK" -%}
{%- set src_hashdiff = "PRODUCT_HASHDIFF" -%}
{%- set src_payload =  get_filtered_columns(ref('raw_product'), except=['SKU_ID', 'LDTS', 'SOURCE_FILE']) -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                   src_payload=src_payload, src_eff=src_eff,
                   src_ldts=src_ldts, src_source=src_source,
                   source_model=source_model) }}
