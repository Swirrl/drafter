#!/bin/bash

if [ -n "$TRAVIS" ] ; then
    echo Setting up travis dependencies...
    #apt-get update  ## This breaks pip/awscli
    apt-get install -y git unzip build-essential apache2-utils wget bsdtar python-pip
    chown -R travis ./travis/*
    chmod +x ./travis/*
    mkdir -p /etc/leiningen/
    mv ./travis/profiles.clj /etc/leiningen/profiles.clj
    export AWS_INSTALL=true ## used to parameterise curl command below...
fi

echo Fetching stardog
curl 'https://raw.githubusercontent.com/Swirrl/stardog/master/consume/setup-stardog?token=AABCpLUdCWZQ-1MW8nm70bQLGqZJfGNeks5csMkIwA%3D%3D' | \
    env STARDOG_BUILD=stardog-5.2.0-45 \
        CREATE_DB=drafter-test-db \
        START_STARDOG=true \
        bash ### <--- bash command here runs curled script
