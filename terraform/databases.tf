resource "snowflake_database" "db" {
  provider = snowflake.sys_admin
  name     = "${var.environment}_DB"
  comment  = "Main dev database"
}

resource "snowflake_grant_privileges_to_account_role" "database_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.db.name
  }
}

resource "snowflake_schema" "staging" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "STAGING"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.staging.name}\""
  }
}


resource "snowflake_schema" "raw_dtv" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "RAW_DTV"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant1" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.raw_dtv.name}\""
  }
}


resource "snowflake_schema" "business_dtv" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "BUSINESS_DTV"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant2" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.business_dtv.name}\""
  }
}

resource "snowflake_schema" "info_delivery" {
  provider            = snowflake.sys_admin
  database            = snowflake_database.db.name
  name                = "INFO_DELIVERY"
  with_managed_access = false
}

resource "snowflake_grant_privileges_to_account_role" "schema_grant3" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.role.name
  on_schema {
    schema_name = "\"${snowflake_database.db.name}\".\"${snowflake_schema.info_delivery.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "tables_grant" {
  provider          = snowflake.security_admin
  privileges        = ["SELECT", "INSERT"]
  account_role_name = snowflake_role.role.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.db.name
    }
  }
}
