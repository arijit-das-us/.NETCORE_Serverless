variable "name" {
  type = string
  description = "The name used to differentiate Azure resources & Airflow deployments in the Sandbox evironments."
}

variable "sandbox" {
  type = bool
  description = "Flag indicating whether this is a sandbox subscription deployment or not.  If not, the 'name' variable will not be used."
  default = true
}

variable "subscription_id" {
  type = string
  description = "The Azure subscription ID to be deployed to."
  default = "200cdc10-c664-4977-adb3-edf02fb85d6c"
}

variable "tenant_id" {
  type = string
  description = "The Azure tenant ID to be deployed to."
  default = "6bf1a438-ef3a-475a-9e51-6ff6e79af305"
}

variable "location" {
  type = string
  description = "The default location to which all Airflow resources will be deployed."
  default = "Central US"
}

variable "vm_akv_name" {
  type = string
  description = "The default location to which all Airflow resources will be deployed."
  default = "de-dev-vm-creds"
}

variable "vm_akv_rg" {
  type = string
  description = "The default location to which all Airflow resources will be deployed."
  default = "Datalake-RG"
}