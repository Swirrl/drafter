#!/usr/bin/env bash

set -o errexit

apt-get install rlwrap
curl -O https://download.clojure.org/install/linux-install-1.10.0.442.sh
chmod +x linux-install-1.10.0.442.sh
sudo ./linux-install-1.10.0.442.sh
