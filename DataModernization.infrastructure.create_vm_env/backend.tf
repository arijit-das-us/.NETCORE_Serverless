terraform {
  backend "azurerm" {
    storage_account_name = "bkcenvterraform"
//    container_name > set upon init
    key = "create-vm-infrastructure.terraform.tfstate"
//    access_key  > set by ENV VAR
  }
}