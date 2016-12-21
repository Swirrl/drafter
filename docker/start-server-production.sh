#!/bin/bash
cd /drafter

# expects to connect to a stardog on the docker-host.
# optionally pass in a DATABASE_NAME (defualt pmd-data)
# optionally pass in a STARDOG_PORT (default 5820)
env SPARQL_UPDATE_ENDPOINT=http://docker-host:${STARDOG_PORT:-5820}/${DATABASE_NAME:-pmd-data}/update \
  SPARQL_QUERY_ENDPOINT=http://docker-host:${STARDOG_PORT:-5820}/${DATABASE_NAME:-pmd-data}/query \
  DRAFTER_BACKEND=drafter.backend.sesame.remote \
  java -Xms500m -Xmx${MAX_HEAP:-4096}m -XX:PermSize=512m -XX:MaxPermSize=512m -jar \
  -Djava.awt.headless=true \
  /drafter/target/drafter.jar
