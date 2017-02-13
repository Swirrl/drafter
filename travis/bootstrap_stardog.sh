#!/bin/bash

# install stardog
sudo mkdir -p /opt/stardog/releases
sudo mkdir -p /var/lib/stardog-home
sudo mkdir -p /opt/stardog/stardog

echo 'Downloading and unzipping Stardog...'
cd /opt/stardog/releases
sudo curl -O https://stardog-versions.s3.amazonaws.com/stardog-4.1.2.zip
sudo unzip stardog-4.1.2.zip

echo 'Adding license...'
sudo cp ./travis/stardog-license-key.bin /var/lib/stardog-home
sudo cp ./travis/stardog-properties /var/lib/stardog-home
sudo ln -s /opt/stardog/releases/stardog-4.1.2 /opt/stardog/stardog

# start stardog
/opt/stardog/stardog/bin/stardog-admin server start --disable-security