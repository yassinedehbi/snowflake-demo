terraform {
  required_providers {
    snowflake = {
      source  = "chanzuckerberg/snowflake"
      version = "0.25.17"
    }
  }

  backend "remote" {
    organization = "ayur"

    workspaces {
      name = "snowflake-demo"
    }
  }
}

provider "snowflake" {
}
