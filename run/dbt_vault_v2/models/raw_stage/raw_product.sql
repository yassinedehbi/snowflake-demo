
  create or replace   view DEV_DB.DBT_VAULT.raw_product
  
   as (
    select * from DEV_DB.STAGING.STG_PRODUCT_RAW
  );

