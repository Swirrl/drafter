#!/bin/bash

mkdir -p dist
cp target/drafter.jar dist/drafter-$TRAVIS_BRANCH-latest.jar
cp target/drafter.jar dist/drafter-private-$TRAVIS_BRANCH-$TRAVIS_BUILD_NUMBER.jar
mkdir -p releases
cp target/drafter.jar releases/drafter-$TRAVIS_TAG-$TRAVIS_BUILD_NUMBER.jar
