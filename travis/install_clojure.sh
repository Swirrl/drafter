#!/usr/bin/env bash

set -o errexit

apt-get install rlwrap

curl -O https://download.clojure.org/install/linux-install-1.10.3.1040.sh
chmod +x linux-install-1.10.3.1040.sh
sudo ./linux-install-1.10.3.1040.sh
