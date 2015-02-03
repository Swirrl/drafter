#!/bin/bash
cd /drafter
java -Xms2048m -Xmx4096m -XX:PermSize=512m -XX:MaxPermSize=512m -jar -Djava.awt.headless=true -Ddrafter.repo.path=/data/drafter-database /drafter/target/drafter.jar
