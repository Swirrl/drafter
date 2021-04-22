#!/usr/bin/env bash

set -o errexit

apt-get update
apt-get install -y build-essential
chown -R travis ./travis/*
chmod +x ./travis/*

./travis/install_clojure.sh
