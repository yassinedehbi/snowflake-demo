Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

-- {%- set yaml_metadata -%}
-- source_model: 'raw_product'

-- derived_columns:
--   RECORD_SOURCE: 'SOURCE_FILE'
--   LOCATION_KEY: 'LOCATION_ID'
--   LOAD_DATE: 'UPDATE_DATE'
-- --   EFFECTIVE_FROM: 'TRANSACTION_DATE'

-- hashed_columns:
--     LOCATION_HASHDIFF:
--         is_hashdiff: true
--         columns:
--             - 'LOCATION_CODE'
--             - 'LOCATION_PLANNING_CODE'
--             - 'CURRENCY_ID'
--             - 'CONCEPT_CODE'
--             - 'CITY_ID'
--             - 'STATE_ID'
--             - 'AREA_ID'
-- {%- endset -%}

-- {% set metadata_dict = fromyaml(yaml_metadata) %}

-- {% set source_model = metadata_dict['source_model'] %}

-- {% set derived_columns = metadata_dict['derived_columns'] %}

-- {% set hashed_columns = metadata_dict['hashed_columns'] %}

-- {{ automate_dv.stage(include_source_columns=true,
--                      source_model=source_model,
--                      derived_columns=derived_columns,
--                      hashed_columns=hashed_columns,
--                      ranked_columns=none) }}