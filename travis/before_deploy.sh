#!/bin/bash

cd `dirname $0`/../drafter

mkdir -p dist

cp target/drafter.jar dist/drafter-$TRAVIS_BRANCH-latest.jar
cp target/drafter.jar dist/drafter-private-$TRAVIS_BRANCH-$TRAVIS_BUILD_NUMBER.jar
mkdir -p releases
cp target/drafter.jar releases/drafter-$TRAVIS_TAG-$TRAVIS_BUILD_NUMBER.jar

cd ..

# setup omni package
cp drafter/target/drafter.jar package/install/drafter-$TRAVIS_BRANCH-$TRAVIS_BUILD_NUMBER.jar
