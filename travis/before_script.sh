#!/bin/bash

sleep 15

env JAVA_HOME=/usr/lib/jvm/java-8-oracle/bin/java \
    ./.omni_cache/install/stardog/install/stardog/bin/stardog-admin db create \
    -n drafter-test-db \
    -o strict.parsing=false \
    -o query.all.graphs=true \
    -o reasoning.schema.graphs='http://publishmydata.com/graphs/reasoning-tbox' \
    -o reasoning.type=SL --

env JAVA_HOME=/usr/lib/jvm/java-8-oracle/bin/java \
    ./.omni_cache/install/stardog/install/stardog/bin/stardog-admin db create \
    -n drafter-client-test \
    -o strict.parsing=false \
    -o query.all.graphs=true \
    -o reasoning.schema.graphs='http://publishmydata.com/graphs/reasoning-tbox' \
    -o reasoning.type=SL --

sleep 10
