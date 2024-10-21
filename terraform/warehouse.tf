resource "snowflake_warehouse" "warehouse" {
  provider = snowflake.security_admin
  name           = "DEV_WH"
  warehouse_size = "xsmall"
  auto_suspend   = 60
}
