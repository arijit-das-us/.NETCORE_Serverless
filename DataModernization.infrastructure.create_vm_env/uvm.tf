#############################
# Providers
#############################

provider "azurerm" {
  version         = "=2.35.0"

  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
  features {}
}

# aliased provider for any deployment to access persistent kv regardless of deployment subscription
# This holds the hardcoded subscription id that the kv resides in and can be accessed at. 
# This KV holds only ad artifacts used for the deployment because these artifacts are subscription independent
provider "azurerm" {
  alias = "auth_kv_source"
  subscription_id = "3d674fc4-1578-48fe-9824-5d3cec7c4dfd"
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
}

provider "azuread" {
  version = "=1.0.0"
}

provider "random" {
  version = "=3.0.0"
}

#############################
# Locals
#############################

locals {
    rg_name = "${var.name}_DE_VM_Build"
    keyvault_name = "${var.name}-${random_string.random_string.result}-akv"
    data_engineering_group_id = "d47d2802-719c-4f9b-94b8-41e7c9066e8f"
}


# data "azurerm_key_vault" "vm-kv" {
#   provider = azurerm.auth_kv_source
#   name = var.vm_akv_name
#   resource_group_name = var.vm_akv_rg
# }

#############################
# Resources
#############################
data "azurerm_client_config" "current" {}

data "http" "myip" {
  url = "https://ipinfo.io/ip"
}

resource "random_string" "random_string" {
  length = 8
  special = false
  upper = false
}

#Create resource group
resource "azurerm_resource_group" "vm_rg" {
  name     = local.rg_name
  location = var.location
}

resource "azurerm_key_vault" "vm_akv" {
  name                     = local.keyvault_name
  resource_group_name      = local.rg_name
  location                 = var.location
  tenant_id                = var.tenant_id
  soft_delete_enabled      = false
  purge_protection_enabled = false
  sku_name                 = "standard"

  # current data source granted read/write access to create secrets
  access_policy {
    object_id = data.azurerm_client_config.current.object_id
    tenant_id = var.tenant_id
    key_permissions = [
      "get",
      "create",
      "delete",
    ]
    secret_permissions = [
      "get",
      "list",
      "set",
      "delete",
    ]
    certificate_permissions = [
      "get",
      "create",
      "delete",
    ]
    storage_permissions = [
      "get",
      "set",
      "delete",
    ]
  }
  depends_on = [azurerm_resource_group.vm_rg]
}

resource "random_password" "vm_administrator_login_password" {
  length      = 14
  min_upper   = 2
  min_lower   = 2
  min_numeric = 2
  special = false
}

resource "azurerm_key_vault_secret" "vm_administrator_login_password" {
    name = "${var.name}vmadmin" 
    value = random_password.vm_administrator_login_password.result
    key_vault_id = azurerm_key_vault.vm_akv.id
}

# Create virtual network
resource "azurerm_virtual_network" "vm_network" {
    name                = "${var.name}-Vnet"
    address_space       = ["10.0.0.0/16"]
    location            = var.location
    resource_group_name = local.rg_name

 

    tags = {
        environment = "Terraform VM Demo"
    }
    depends_on = [azurerm_resource_group.vm_rg]
}

 

# Create subnet
resource "azurerm_subnet" "vm_subnet" {
    name                 = "${var.name}-subnet"
    resource_group_name  = local.rg_name
    virtual_network_name = azurerm_virtual_network.vm_network.name
    address_prefixes       = ["10.0.1.0/24"]
    depends_on = [azurerm_resource_group.vm_rg]
}

 

# Create public IPs
resource "azurerm_public_ip" "vm_ip" {
    name                         = "${var.name}-ip"
    location                     = var.location
    resource_group_name          = local.rg_name
    allocation_method            = "Static"

 

    tags = {
        environment = "Terraform Demo"
    }
    depends_on = [azurerm_resource_group.vm_rg]
}



# Create Network Security Group and rule
resource "azurerm_network_security_group" "vm_nsg" {
    name                = "${var.name}_NetworkSecurityGroup"
    location            = var.location
    resource_group_name = local.rg_name

 

    security_rule {
        name                       = "SSH"
        priority                   = 299
        direction                  = "Inbound"
        access                     = "Allow"
        protocol                   = "Tcp"
        source_port_range          = "*"
        destination_port_range     = "22"
        source_address_prefix      = "*"
        destination_address_prefix = "*"
    }
    security_rule {
        name                       = "RDP"
        priority                   = 300
        direction                  = "Inbound"
        access                     = "Allow"
        protocol                   = "Tcp"
        source_port_range          = "*"
        destination_port_range     = "3389"
        source_address_prefix      = "*"
        destination_address_prefix = "*"
    }
    security_rule {
        name                       = "misc"
        priority                   = 301
        direction                  = "Inbound"
        access                     = "Allow"
        protocol                   = "Tcp"
        source_port_range          = "*"
        destination_port_range     = "80"
        source_address_prefix      = "*"
        destination_address_prefix = "*"
    }

 

    tags = {
        environment = "Terraform Demo"
    }
    depends_on = [azurerm_resource_group.vm_rg]
}

 

# Create network interface
resource "azurerm_network_interface" "vm_nic" {
    name                      = "${var.name}_NIC"
    location                  = var.location
    resource_group_name       = local.rg_name

 

    ip_configuration {
        name                          = "${var.name}_NicConfiguration"
        subnet_id                     = azurerm_subnet.vm_subnet.id
        private_ip_address_allocation = "Dynamic"
        public_ip_address_id          = azurerm_public_ip.vm_ip.id
    }

 

    tags = {
        environment = "Terraform Demo"
    }
    depends_on = [azurerm_resource_group.vm_rg]
}

 

# Connect the security group to the network interface
resource "azurerm_network_interface_security_group_association" "vmsecint" {
    network_interface_id      = azurerm_network_interface.vm_nic.id
    network_security_group_id = azurerm_network_security_group.vm_nsg.id
    depends_on = [azurerm_resource_group.vm_rg]
}
 

# Create (and display) an SSH key
# resource "tls_private_key" "example_ssh" {
#   algorithm = "RSA"
#   rsa_bits = 4096
#   depends_on = [azurerm_resource_group.vm_rg]
# }
# output "tls_private_key" { value = tls_private_key.example_ssh.private_key_pem }


data "azurerm_public_ip" "read_vm_ip" {
  name = azurerm_public_ip.vm_ip.name
  resource_group_name = azurerm_public_ip.vm_ip.resource_group_name
  depends_on = [azurerm_public_ip.vm_ip]
}

data "azurerm_key_vault_secret" "vm_akv_secret_pull" {
  name = "${var.name}vmadmin"
  key_vault_id = azurerm_key_vault.vm_akv.id
  depends_on = [azurerm_key_vault_secret.vm_administrator_login_password]
}

# Create virtual machine
resource "azurerm_linux_virtual_machine" "vm" {
    name                  = "${var.name}-vm"
    location              = var.location
    resource_group_name   = local.rg_name
    network_interface_ids = [azurerm_network_interface.vm_nic.id]
    size                  = "Standard_D4as_v4"

 

    os_disk {
        name              = "${var.name}-OsDisk"
        caching           = "ReadWrite"
        storage_account_type = "Standard_LRS"
    }

 

    source_image_reference {
        publisher = "Canonical"
        offer     = "0001-com-ubuntu-server-focal"
        sku       = "20_04-lts"
        version   = "latest"
    }

    admin_username = var.name
    disable_password_authentication = false
    admin_password = random_password.vm_administrator_login_password.result
 

    tags = {
        environment = "Terraform Demo"
    }
    provisioner "file" {
      source = "ubuntu_config.sh"
      destination = "/tmp/ubuntu_config.sh"
      connection {
            type = "ssh"
            user = var.name
            host = data.azurerm_public_ip.read_vm_ip.ip_address
            password = data.azurerm_key_vault_secret.vm_akv_secret_pull.value
            timeout="20m"
      }
    }
    provisioner "remote-exec" {
        inline = [
        "chmod +x /tmp/ubuntu_config.sh",
        "/tmp/ubuntu_config.sh"
        ]
        connection {
            type = "ssh"
            user = var.name
            host = data.azurerm_public_ip.read_vm_ip.ip_address
            password = data.azurerm_key_vault_secret.vm_akv_secret_pull.value
            timeout="20m"
        }
    }
    depends_on = [azurerm_resource_group.vm_rg, azurerm_network_security_group.vm_nsg ]
}



output "public_ip" { 
    value = "Your VM is reachable via Remote Desktop at ${data.azurerm_public_ip.read_vm_ip.ip_address} "
    depends_on = [azurerm_public_ip.vm_ip ]
}
