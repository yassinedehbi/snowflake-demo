
  create or replace   view DEV_DB.DBT_VAULT.raw_sales
  
   as (
    select * from DEV_DB.STAGING.STG_SALES_RAW
  );

