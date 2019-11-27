#!/bin/bash

cd {{omni/install-dir}}

set -o allexport
source dev-settings.env

java -jar ./drafter.jar drafter-dev-basic-auth-memory-db.edn stasher-off.edn &

#TODO: wait for drafter to become available
echo $! > drafter.pid
