#!/bin/bash

apt-get update
apt-get install -y git unzip build-essential apache2-utils wget bsdtar python-pip
chown -R travis ./travis/*
chmod +x ./travis/*
mkdir -p /etc/leiningen/
mv ./travis/profiles.clj /etc/leiningen/profiles.clj
