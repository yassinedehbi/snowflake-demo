resource "snowflake_warehouse" "warehouse" {
  name           = "DEV_WH"
  warehouse_size = "xsmall"
  auto_suspend   = 60
}
