{%- set source_model = "v_stg_sales" -%}
{%- set src_pk = "SALES_HK" -%}
{%- set src_hashdiff = "SALES_HASHDIFF" -%}
{%- set src_payload =  get_filtered_columns(ref('raw_sales'), except=['TRANSACTION_SALES', 'SKU_ID', 'LOCATION_ID', 'LDTS', 'SOURCE_FILE']) -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                   src_payload=src_payload, src_eff=src_eff,
                   src_ldts=src_ldts, src_source=src_source,
                   source_model=source_model) }}
