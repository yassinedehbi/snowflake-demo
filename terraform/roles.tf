resource "snowflake_role" "role" {
  provider = snowflake.security_admin
  name     = "${var.environment}_ROLE"
}

resource "snowflake_grant_account_role" "grants" {
  provider  = snowflake.security_admin
  role_name = snowflake_role.role.name
  user_name = "SC_USER"
}