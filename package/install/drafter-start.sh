#! /bin/bash

cd {{omni/install-dir}}

DRAFTER_JVM_OPTS="-Xmx4g \
                  -Dcom.sun.management.jmxremote.ssl=false \
                  -Dcom.sun.management.jmxremote.authenticate=false \
                  -Dcom.sun.management.jmxremote.port=3007 \
                  -Dhttp.maxConnections=60 \
                  -Dorg.eclipse.jetty.server.Request.maxFormContentSize=41943040 \
                  -Dlog4j2.formatMsgNoLookups=true \
                  -Dlog4j.configurationFile=log4j2.xml"

java ${DRAFTER_JVM_OPTS} -cp 'lib/*:drafter.jar' clojure.main -m drafter.main drafter-prod.edn
