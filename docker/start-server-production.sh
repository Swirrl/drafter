#!/bin/bash
cd /drafter
java -jar -Djava.awt.headless=true -Ddrafter.repo.path=/var/lib/drafter-database /drafter/target/drafter-0.1.0-SNAPSHOT-standalone.jar
