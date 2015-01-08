#!/bin/bash
cd /drafter
java -jar -Djava.awt.headless=true -Ddrafter.repo.path=/data/drafter-database /drafter/target/drafter.jar
