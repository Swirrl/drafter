FROM ubuntu:14.04

MAINTAINER Ric Roberts "ric@swirrl.com"

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -qq
RUN apt-get upgrade -qq
RUN apt-get install -qq -y software-properties-common wget
RUN add-apt-repository ppa:openjdk-r/ppa -y
RUN apt-get update -qq
RUN apt-get -qq -y install --no-install-recommends openjdk-8-jdk

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
