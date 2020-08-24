#!/bin/bash

cd {{omni/install-dir}}

set -o allexport
source dev-settings.env

java -jar ./drafter.jar drafter-dev-auth0.edn stasher-off.edn init-public-endpoint.edn &

#TODO: wait for drafter to become available
echo $! > drafter.pid
