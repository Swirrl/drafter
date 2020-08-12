#!/usr/bin/env bash

set -o errexit

apt-get install rlwrap
curl -O https://download.clojure.org/install/linux-install-1.10.1.561.sh
chmod +x linux-install-1.10.1.561.sh
sudo ./linux-install-1.10.1.561.sh
