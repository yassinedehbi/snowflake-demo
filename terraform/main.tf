terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
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
