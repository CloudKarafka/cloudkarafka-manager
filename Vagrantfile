# -*- mode: ruby -*-
# vi: set ft=ruby :

GO_BOOSTRAP_SCRIPT = <<-SCRIPT
sudo apt-get update
sudo apt-get install --yes openjdk-8-jre-headless
sudo snap install --classic --channel=1.13/stable go
SCRIPT

Vagrant.configure("2") do |config|
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "512"
    vb.cpus = 1
  end

  config.vm.provision "shell", inline: GO_BOOSTRAP_SCRIPT

  config.vm.define "ubuntu18" do |node|
    node.vm.box = "ubuntu/bionic64"
  end
end
