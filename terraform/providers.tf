provider "snowflake" {
  alias = "security_admin"
  role  = "SECURITYADMIN"
}

provider "snowflake" {
    alias = "sys_admin"
    role = "SYSADMIN"
}

