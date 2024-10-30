resource "snowflake_warehouse" "warehouse" {
  provider       = snowflake.sys_admin
  name           = "${var.environment}_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = 60
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_grant" {
  privileges        = ["USAGE"]
  provider          = snowflake.sys_admin
  account_role_name = snowflake_role.role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.warehouse.name
  }
}