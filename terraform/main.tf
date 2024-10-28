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
      prefix = "snowflake-demo-"

    }
  }
}

provider "snowflake" {
}
