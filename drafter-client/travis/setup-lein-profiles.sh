#!/bin/bash

sudo chown -R travis ./travis/*
sudo chmod +x ./travis/*
sudo mkdir -p /etc/leiningen/
sudo mv ./travis/profiles.clj /etc/leiningen/profiles.clj