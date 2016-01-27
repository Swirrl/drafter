(defproject drafter "0.4.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://github.com/Swirrl/drafter"
  :license {:name "Proprietary & Commercially Licensed Only"
            :url "http://swirrl.com/"}

  :repositories [["snapshots" {:url "s3p://swirrl-jars/snapshots/"
                               :sign-releases false
                               :releases false}]
                 ["releases" {:url "s3p://swirrl-jars/releases/"
                              :sign-releases true
                              :snapshots false}]]

  :dependencies [
                 ;; NOTE jena 3.0.0-SNAPSHOT was compiled from source with maven
                 ;; and pushed into our private repo at commit:
                 ;; d58c1a1abc7dfb2a58ce5b8c04e176940fecbb9a
                 ;;
                 ;; This is necessary for the new rewriting stuff.
                 [org.apache.jena/jena-arq "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api
                                                                         com.fasterxml.jackson.core/jackson-core
                                                                         org.slf4j/jcl-over-slf4j
                                                                         org.slf4j/slf4j-api
                                                                         org.apache.httpcomponents/httpclient]]

                 [org.apache.jena/jena-core "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-base "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-iri "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]

                 [org.clojure/clojure "1.6.0"]
                 [me.raynes/fs "1.4.6"] ; ;filesystem utils
                 [lib-noir "0.8.4" :exclusions [compojure org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [ring "1.3.2" :exclusions [org.clojure/java.classpath]]
                 [ring-server "0.3.1"]
                 [wrap-verbs "0.1.1"]
                 [selmer "0.6.9"]

                 [swirrl/lib-swirrl-server "0.2.0-SNAPSHOT" :exclusions [clout org.clojure/java.classpath]]
                 ;; TODO:
                 ;;
                 ;; When this sesame bug about streaming sparql XML
                 ;; results is fixed:
                 ;;
                 ;; https://openrdf.atlassian.net/browse/SES-2119
                 ;;
                 ;; we can remove this exclusion.
                 [grafter "0.6.0"]
                 [org.openrdf.sesame/sesame-queryresultio-sparqlxml "2.8.9"]

                 [grafter/vocabularies "0.1.3"]

                 [org.openrdf.sesame/sesame-queryrender "2.8.9" :exclusions [org.openrdf.sesame/sesame-http-client]]
                 [org.openrdf.sesame/sesame-runtime "2.8.9" :exclusions [org.openrdf.sesame/sesame-http-client]]

                 ;; STOMP over sesames version of their http client lib with a release that patches SES-2368
                 ;; see here for the source code that built this release:
                 ;;
                 ;; https://github.com/RickMoynihan/sesame/tree/connection-pool-timeout

                 [swirrl/sesame-http-client "2.8.9"]

                 [clj-logging-config "1.9.12"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [org.slf4j/slf4j-log4j12 "1.7.9" :exclusions [log4j org.slf4j/slf4j-api]]
                 [ring-middleware-accept "2.0.2"]
                 [environ "1.0.0"]]

  :java-source-paths ["src-java"]
  :resource-paths ["resources"]
  :pedantic :abort

  :repl-options {:init-ns drafter.repl
                 :init (-main)
                 :timeout 180000}

  :plugins [[lein-ring "0.8.10" :exclusions [org.clojure/clojure]]
            [lein-environ "1.0.0"]
            [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]
            [lein-test-out "0.3.1" :exclusions [org.clojure/tools.namespace]]]

  :ring {:handler drafter.handler/app
         :init    drafter.handler/init
         :destroy drafter.handler/destroy}

  :aliases {"reindex" ["run" "-m" "drafter.backend.sesame-native/reindex"]}

  :target-path "target/%s" ;; ensure profiles don't pollute each other with
                           ;; compiled classes etc...

  :clean-targets [:target-path :compile-path]

  :profiles
  {
   :uberjar {:aot :all
             :main drafter.repl
             :uberjar [:swirrl-private-repos
                        { :project-config-stuff "goes here"}]}

   :production {:ring {:open-browser? false
                       :stacktraces?  false
                       :auto-reload?  false}}



   :dev {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                   [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]]

         :dependencies [[ring-mock "0.1.5"]
                        [com.aphyr/prism "0.1.1" :exclusions [org.clojure/clojure]]
                        [org.clojure/data.json "0.2.5"]
                        [clojure-csv/clojure-csv "2.0.1"]
                        [ring/ring-devel "1.3.2" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]]

         :env {:dev true}

         ;:jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]
         ;;:jvm-opts ["-Djava.awt.headless=true" "-XX:+UnlockCommercialFeatures"  "-XX:+FlightRecorder" "-XX:FlightRecorderOptions=defaultrecording=true,disk=true"]
         }
   }


  :jvm-opts ["-Djava.awt.headless=true"

             ;; Use this property to control number
             ;; of connections in the SPARQLRepository connection pool:
             ;;
             ;;"-Dhttp.maxConnections=1"

             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]

  ;NOTE: expected JVM version to run against is defined in the Dockerfile
  :javac-options ["-target" "7" "-source" "7"]
  :min-lein-version "2.5.0"
  )
