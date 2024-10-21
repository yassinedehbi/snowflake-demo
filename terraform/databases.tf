resource "snowflake_database" "dev_db" {
  provider = snowflake.sys_admin
  name     = "DEV_DB"
  comment  = "Main dev database"
}

resource "snowflake_grant_privileges_to_account_role" "database_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.dev_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dev_db.name
  }
}

resource "snowflake_schema" "staging" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.dev_db.name
  name                = "STAGING"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.dev_role.name
  on_schema {
    schema_name = "\"${snowflake_database.dev_db.name}\".\"${snowflake_schema.staging.name}\""
  }
}

