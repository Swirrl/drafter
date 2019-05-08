#!/bin/bash

apt-get update
apt-get install -y git unzip build-essential apache2-utils wget bsdtar python-pip
chown -R travis ./travis/*
chmod +x ./travis/*
mkdir -p /etc/leiningen/
mv ./travis/profiles.clj /etc/leiningen/profiles.clj

./travis/install_clojure.sh

# start ssh agent and add key so clojure tools can clone git repos
ssh-agent
ssh-add

# install service dependencies
clojure -M:omni install-dependencies

# start services
./.omni_cache/install/stardog/install/dev-start.sh
./.omni_cache/install/mongodb/install/dev-start.sh
