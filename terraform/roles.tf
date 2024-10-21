resource "snowflake_role" "dev_role" {
  provider = snowflake.security_admin
  name     = "DEV_ROLE"
}

resource "snowflake_grant_account_role" "grants" {
  provider  = snowflake.security_admin
  role_name = snowflake_role.dev_role.name
  user_name = "VISEO"
}