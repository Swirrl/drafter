#!/bin/bash

set -o errexit

sleep 15

env JAVA_HOME=/usr/lib/jvm/java-8-oracle/bin/java \
    ./.omni_cache/install/stardog/install/stardog/bin/stardog-admin db create \
    -n drafter-test-db \
    -c ./travis/stardog.properties

env JAVA_HOME=/usr/lib/jvm/java-8-oracle/bin/java \
    ./.omni_cache/install/stardog/install/stardog/bin/stardog-admin db create \
    -n drafter-client-test \
    -c ./travis/stardog.properties

sleep 10
