#! /bin/bash

cd {{omni/install-dir}}
env $(cat config.env | xargs) java -jar ./drafter.jar
