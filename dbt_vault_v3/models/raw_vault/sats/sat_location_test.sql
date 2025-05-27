{%- set source_model = "v_stg_location" -%}
{%- set src_pk = "LOCATION_HK" -%}
{%- set src_hashdiff = "LOCATION_HASHDIFF" -%}
{%- set src_payload =  get_filtered_columns(ref('raw_location'), except=['LOCATION_ID', 'LDTS', 'SOURCE_FILE']) -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                   src_payload=src_payload, src_eff=src_eff,
                   src_ldts=src_ldts, src_source=src_source,
                   source_model=source_model) }}
