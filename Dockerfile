FROM ubuntu:14.04

MAINTAINER Ric Roberts "ric@swirrl.com"

RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive apt-get -y install openjdk-7-jdk wget

ADD docker/start-server-production.sh /usr/bin/start-server-production
RUN chmod +x /usr/bin/start-server-production

ADD ./ /drafter
ADD https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein /usr/bin/lein
RUN chmod a+x /usr/bin/lein

# copy production log config
ADD docker/log-config.edn /drafter/log-config.edn

WORKDIR /drafter
RUN /usr/bin/lein uberjar

# Mount logs
VOLUME ["/drafter/logs", "/var/lib/drafter-database"]

# serve nginx
EXPOSE 3001

CMD ["/usr/bin/start-server-production"]