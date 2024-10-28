resource "snowflake_warehouse" "warehouse" {
  provider       = snowflake.sys_admin
  name           = "${environment}_WH"
  warehouse_size = "xsmall"
  auto_suspend   = 60
}
