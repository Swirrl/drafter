#!/bin/bash
cd /drafter

#expects a stardog to be linked.
env SPARQL_UPDATE_ENDPOINT=http://$STARDOG_PORT_5820_TCP_ADDR:5820/pmd-data/update \
  SPARQL_QUERY_ENDPOINT=http://$STARDOG_PORT_5820_TCP_ADDR:5820/pmd-data/query \
  java -Xms2048m -Xmx${MAX_HEAP:-4096}m -XX:PermSize=512m -XX:MaxPermSize=512m -jar \
  -Djava.awt.headless=true \
  /drafter/target/drafter.jar