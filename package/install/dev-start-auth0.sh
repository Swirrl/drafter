#!/bin/bash

cd {{omni/install-dir}}

set -o allexport
source dev-settings.env

java -cp 'lib/*:drafter.jar' clojure.main -m drafter.main drafter-dev-auth0.edn stasher-off.edn &

#TODO: wait for drafter to become available
echo $! > drafter.pid
