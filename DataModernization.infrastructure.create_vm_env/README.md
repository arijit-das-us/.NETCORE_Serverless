

# Data Engineering Environment Deployment
* First things first you will want to create a container in the storage account bkcenvterraform which is located in nonprod, resource group terraform-rg, the naming convention for the container is $name-vm. 
* Up next, open a terminal in the directory you have this project in, you will need to set the env var ARM_ACCESS_KEY= <container access key>.
* Next, log in to the Azure CLI utility via `az login`. This will open a new tab in your web browser to get the `az` command authenticated with the Azure Portal.
* From there, simply run terraform init, and terraform apply. 
This should create all of the infrastructure you need for your development environment and you can then reach the new desktop environment through the public ip displayed once the terraform deployment is complete.
## Connecting to this desktop is relatively straight forward:
### Accessing the VM from BlueKC Machines:
* As of right now, the only way to access your ubuntu vm from our BlueKC machines is to disable global protect, use Windows Remote Desktop, and enter the above mentioned IP address. 
* The username that you will be using is the same name that you entered for your terraform deployment
* The password will only be accessible by you in the keyvault within the newly created $NAME_DE_VM_Build resource group
    * This password will be under secrets and it will be named $NAMEvmadmin.
* Once you have entered these credentials and made sure that global protect is disabled, you should be able to access your VM
### Accessing the VM from other Machines:
* This is much simpler, follow the same criteria above for connecting to your vm but use any remote desktop connection you wish

### Note for modications to ubuntu_config.sh 
* If modifications to this file are being made on a windows machine you will need to ensure that the end of line sequence is set as LF and not CRLF. This is a windows/ linux mismatch and the file will not work properly otherwise

