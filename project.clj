(defproject drafter "2.0.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://github.com/Swirrl/drafter"
  :license {:name "Proprietary & Commercially Licensed Only"
            :url "http://swirrl.com/"}

  :repositories [["snapshots" {:url "s3p://swirrl-jars/snapshots/"
                               :sign-releases false
                               :username :env
                               :passphrase :env
                               :releases false}]
                 ["releases" {:url "s3p://swirrl-jars/releases/"
                              :sign-releases true
                              :username :env
                              :passphrase :env
                              :snapshots false}]]

  :dependencies [
                 [org.apache.jena/jena-arq "3.0.1" :exclusions [org.slf4j/slf4j-api
                                                                         com.fasterxml.jackson.core/jackson-core
                                                                         org.slf4j/jcl-over-slf4j
                                                                         org.apache.httpcomponents/httpclient]]

                 [org.apache.jena/jena-core "3.0.1" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-base "3.0.1" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-iri "3.0.1" :exclusions [org.slf4j/slf4j-api]]

                 [org.clojure/clojure "1.8.0"]

                 [me.raynes/fs "1.4.6"] ;; filesystem utils
                 [lib-noir "0.9.9" :exclusions [compojure org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [ring "1.4.0" :exclusions [org.clojure/java.classpath]]
                 [ring/ring-core "1.4.0"]
                 [ring-server "0.4.0"]
                 [wrap-verbs "0.1.1"]
                 [selmer "0.6.9"]
                 [com.novemberain/monger "3.0.2"]

                 [swirrl/lib-swirrl-server "0.6.3" :exclusions [clout org.clojure/java.classpath]]

                 [buddy/buddy-core "0.9.0"]
                 [buddy/buddy-auth "0.9.0"]
                 [org.mindrot/jbcrypt "0.3m"]

                 [grafter "0.7.4"]
                 [org.openrdf.sesame/sesame-queryresultio-sparqlxml "2.8.9"]

                 [grafter/vocabularies "0.1.3"]
                 [grafter/url "0.2.1"]

                 [org.openrdf.sesame/sesame-queryrender "2.8.9" :exclusions [org.openrdf.sesame/sesame-http-client]]
                 [org.openrdf.sesame/sesame-runtime "2.8.9" :exclusions [org.openrdf.sesame/sesame-http-client]]

                 ;; STOMP over sesames version of their http client lib with a release that patches SES-2368
                 ;; see here for the source code that built this release:
                 ;;
                 ;; https://github.com/RickMoynihan/sesame/tree/connection-pool-timeout

                 [swirrl/sesame-http-client "2.8.9-with-connection-pool-and-url-fix"]

                 [com.taoensso/tower "2.0.2"]
                 [org.slf4j/slf4j-log4j12 "1.7.9" :exclusions [log4j org.slf4j/slf4j-api]]
                 [ring-middleware-accept "2.0.3"]
                 [environ "1.0.0"]

                 [prismatic/schema "1.0.4"]
                 ;;[perforate "0.3.4"] ;; include perforate and criterium in repl environments
                 ;;[criterium "0.4.3"] ;; for easy benchmarking
                 ;;[clj-http "1.1.0"]
                 [drafter-client "0.3.6-SNAPSHOT"]

                 [metosin/ring-swagger-ui "2.1.4-0"]

                 [com.sun.mail/javax.mail "1.5.5"]]

  :java-source-paths ["src-java"]
  :resource-paths ["resources"]
  :pedantic :abort

  :repl-options {:init-ns drafter.repl
                 :init (-main)
                 :timeout 180000}

  :plugins [[lein-ring "0.8.10" :exclusions [org.clojure/clojure]]
            [lein-environ "1.0.0"]
            [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]
            [lein-test-out "0.3.1" :exclusions [org.clojure/tools.namespace]]
            [perforate "0.3.4"]]

  :ring {:handler drafter.handler/app
         :init    drafter.handler/init
         :destroy drafter.handler/destroy
         :open-browser? false }

  :aliases {"reindex" ["run" "-m" "drafter.backend.sesame-native/reindex"]}

  :target-path "target/%s" ;; ensure profiles don't pollute each other with
  ;; compiled classes etc...

  :clean-targets [:target-path :compile-path]

  :profiles
  {

   :uberjar {:aot :all
             :main drafter.repl
             }

   :production {:ring {:open-browser? false
                       :stacktraces?  false
                       :auto-reload?  false}}


   :perforate { :dependencies [[perforate "0.3.4"] ;; include perforate and criterium in repl environments
                               [criterium "0.4.3"] ;; for easy benchmarking
                               [clj-http "1.1.0"]
                               [drafter-client "0.3.6-SNAPSHOT"]
                               [grafter "0.6.0-alpha5"]
                               ]}

   :dev [:dev-common :dev-overrides]

   :dev-common {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                          [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]]

                :dependencies [[ring-mock "0.1.5"]
                               [com.aphyr/prism "0.1.1" :exclusions [org.clojure/clojure]]
                               [org.clojure/data.json "0.2.5"]
                               [clojure-csv/clojure-csv "2.0.1"]
                               [ring/ring-devel "1.3.2" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]
                               ;;[perforate "0.3.4"] ;; include perforate and criterium in repl environments
                               ;;[criterium "0.4.3"] ;; for easy benchmarking
                               ;;[clj-http "1.1.0"]
                               ;;[drafter-client "0.3.6-SNAPSHOT"]
                               [prismatic/schema "1.0.4"]]

                ;;:env {:dev true}
                ;;:jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]
                ;;:jvm-opts ["-Djava.awt.headless=true" "-XX:+UnlockCommercialFeatures"  "-XX:+FlightRecorder" "-XX:FlightRecorderOptions=defaultrecording=true,disk=true"]
                }
   }


  :jvm-opts ["-Djava.awt.headless=true"

             ;; Use this property to control number
             ;; of connections in the SPARQLRepository connection pool:
             ;;
             ;;"-Dhttp.maxConnections=1"

             ;;"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
             ]

                                        ;NOTE: expected JVM version to run against is defined in the Dockerfile
  :javac-options ["-target" "7" "-source" "7"]
  :min-lein-version "2.5.0"

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ;;["vcs" "push"]
                  ]


  )
