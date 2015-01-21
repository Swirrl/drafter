FROM ubuntu:14.04

MAINTAINER Ric Roberts "ric@swirrl.com"

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive apt-get -y install openjdk-7-jdk wget

ADD docker/start-server-production.sh /usr/bin/start-server-production
RUN chmod +x /usr/bin/start-server-production

ADD ./ /drafter

# copy production log config
ADD docker/log-config.edn /drafter/log-config.edn

# Mount logs
VOLUME ["/drafter/logs", "/data/drafter-database"]

# serve nginx
EXPOSE 3001

CMD ["/usr/bin/start-server-production"]