sudo apt-get update

sudo DEBIAN_FRONTEND=noninteractive apt-get -yq install xfce4

sudo apt-get -y install xrdp && sudo systemctl enable xrdp

echo xfce4-session >~/.xsession

sudo service xrdp restart

sudo apt -y install flatpak

curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add - 

sudo apt-add-repository "deb [arch=$(dpkg --print-architecture)] https://apt.releases.hashicorp.com $(lsb_release -cs) main"

sudo apt install terraform

sudo apt-get -y install apt-transport-https ca-certificates curl gnupg-agent software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

sudo apt-get update

sudo apt-get -y install docker-ce docker-ce-cli containerd.io

curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

sudo apt -y install firefox