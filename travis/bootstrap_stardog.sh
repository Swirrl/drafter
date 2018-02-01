#!/bin/bash

export WORKING_DIR=`pwd`
echo "> Working dir: $WORKING_DIR"

# install stardog
mkdir -p /opt/stardog/releases
mkdir -p /var/lib/stardog-home
mkdir -p /opt/stardog/stardog

export STARDOG_HOME=/var/lib/stardog-home
echo "> Stardog home: $STARDOG_HOME"

echo '> Downloading and unzipping Stardog...'
cd /opt/stardog/releases
sudo curl -O https://stardog-versions.s3.amazonaws.com/$STARDOG_VERSION.zip
sudo unzip $STARDOG_VERSION.zip
unzip $STARDOG_VERSION.zip
cd $WORKING_DIR

echo "> Permissions.."
chown -R travis /opt/stardog/releases
chmod +wx /opt/stardog/releases
chown -R travis $STARDOG_HOME
chmod +wx $STARDOG_HOME

echo '> Adding license...'
cp $WORKING_DIR/travis/stardog-license-key.bin /var/lib/stardog-home
cp $WORKING_DIR/travis/stardog.properties /var/lib/stardog-home
ln -s /opt/stardog/releases/$STARDOG_VERSION /opt/stardog/stardog

# start stardog
/opt/stardog/stardog/$STARDOG_VERSION/bin/stardog-admin server start --disable-security
