#!/bin/bash

export WORKING_DIR=`pwd`
echo "> Working dir: $WORKING_DIR"

# install stardog
sudo mkdir -p /opt/stardog/releases
sudo mkdir -p /var/lib/stardog-home
sudo mkdir -p /opt/stardog/stardog

export STARDOG_HOME=/var/lib/stardog-home
echo "> Stardog home: $STARDOG_HOME"

echo '> Downloading and unzipping Stardog...'
cd /opt/stardog/releases
sudo curl -O https://stardog-versions.s3.amazonaws.com/stardog-4.1.2.zip
sudo unzip stardog-4.1.2.zip
cd $WORKING_DIR

echo "> Permissions.."
sudo chown -R travis /opt/stardog/releases
sudo chmod +wx /opt/stardog/releases
sudo chown -R travis $STARDOG_HOME
sudo chmod +wx $STARDOG_HOME

echo '> Adding license...'
sudo cp $WORKING_DIR/travis/stardog-license-key.bin /var/lib/stardog-home
sudo cp $WORKING_DIR/travis/stardog.properties /var/lib/stardog-home
sudo ln -s /opt/stardog/releases/stardog-4.1.2 /opt/stardog/stardog

# start stardog
env STARDOG_JAVA_ARGS="-Xms12g -Xmx12g" /opt/stardog/stardog/stardog-4.1.2/bin/stardog-admin server start --disable-security