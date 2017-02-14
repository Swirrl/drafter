VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "geerlingguy/ubuntu1604"
  config.vm.network :private_network, ip: "192.168.88.8"
  config.vm.hostname = "packagey"
  #config.ssh.insert_key = false

  config.vm.provider :virtualbox do |v|
    v.memory = 1024
  end
end
