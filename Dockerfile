ah FROM ubuntu:14.04

MAINTAINER Ric Roberts "ric@swirrl.com"

RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt-get update -qq
RUN apt-get upgrade -qq
RUN apt-get -qq -y install openjdk-8-jdk wget

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
