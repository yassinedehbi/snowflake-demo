
  create or replace   view DEV_DB.DBT_VAULT.raw_location
  
   as (
    select distinct * from DEV_DB.STAGING.STG_LOCATION_RAW
  );

