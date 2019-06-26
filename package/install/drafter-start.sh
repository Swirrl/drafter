#! /bin/bash

cd {{omni/install-dir}}
java -Xmx4g -Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.port=3007 \
-Dhttp.maxConnections=60 \
-Dorg.eclipse.jetty.server.Request.maxFormContentSize=41943040 \
-jar {{omni/install-dir}}/drafter.jar
