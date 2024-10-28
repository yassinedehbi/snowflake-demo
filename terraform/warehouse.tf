resource "snowflake_warehouse" "warehouse" {
  provider       = snowflake.sys_admin
  name           = "${var.environment}_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = 60
}
