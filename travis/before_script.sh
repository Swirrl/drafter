#!/bin/bash

sleep 15

env JAVA_HOME=/usr/lib/jvm/java-8-oracle/bin/java /opt/stardog/stardog/stardog-4.1.2/bin/stardog-admin db create -n drafter-test-db -t D -o strict.parsing=false -o query.all.graphs=true -o reasoning.type=none --

sleep 10
