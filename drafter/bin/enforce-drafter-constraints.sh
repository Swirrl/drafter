#!/bin/bash

set -e # force abort if any command fails

if [ -z "$1" ] ; then
    echo "Usage: $0 <db-name>"
    exit
fi

ROOT_DIR=`dirname $0`/..
DRAFTER_CONSTRAINTS=$ROOT_DIR/resources/drafter/state-graph-constraints.ttl
DATABASE_NAME=$1

echo Adding constraints from file: $DRAFTER_CONSTRAINTS to $DATABASE_NAME

# Assumes $STARDOG/bin is on the path

stardog-admin icv add $DATABASE_NAME $DRAFTER_CONSTRAINTS
stardog-admin db offline --timeout 0 $DATABASE_NAME #take the database offline
stardog-admin metadata set -o icv.enabled=true $DATABASE_NAME #enable ICV
stardog-admin db online $DATABASE_NAME #put the database online
