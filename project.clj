(defproject drafter "2.1.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://github.com/Swirrl/drafter"
  :license {:name "Proprietary & Commercially Licensed Only"
            :url "http://swirrl.com/"}

  :dependencies [[buddy/buddy-auth "0.9.0"]
                 [buddy/buddy-core "0.9.0"]
                 [clj-yaml "0.4.0"] ;; for loading our Swagger schemas

                 ;; Lock dependency of jackson to a version that
                 ;; works with sesame's sparql json results renderer
                 ;; and the scjsv json schema validator.
                 ;;
                 ;; NOTE: When we upgrade sesame to RDF4j we can possibly
                 ;; drop this override.
                 [com.fasterxml.jackson.core/jackson-core "2.6.7"]
                 [com.novemberain/monger "3.0.2"]
                 [com.sun.mail/javax.mail "1.5.5"]
                 [com.taoensso/tower "2.0.2"]
                 [drafter-client "0.3.6-SNAPSHOT"]
                 [environ "1.0.0"]
                 [grafter "0.7.5"]
                 [grafter/url "0.2.1"]
                 [grafter/vocabularies "0.1.3"]
                 [lib-noir "0.9.9" :exclusions [compojure org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [me.raynes/fs "1.4.6"] ;; filesystem utils
                 [metosin/ring-swagger-ui "2.1.4-0"]
                 [metosin/scjsv "0.3.0"] ;; for validating our Swagger/JSON schemas


                 ;; Use JENA for our query rewriting
                 [org.apache.jena/jena-arq "3.1.1" :exclusions [org.slf4j/slf4j-api
                                                                org.slf4j/jcl-over-slf4j
                                                                org.apache.httpcomponents/httpclient]]
                 [org.apache.jena/jena-base "3.1.1" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-core "3.1.1" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-iri "3.1.1" :exclusions [org.slf4j/slf4j-api]]
                 [org.clojure/clojure "1.8.0"]
                 [org.mindrot/jbcrypt "0.3m"]
                 [org.openrdf.sesame/sesame-queryrender "2.8.11"]
                 [org.openrdf.sesame/sesame-queryresultio-sparqlxml "2.8.11"]
                 [org.openrdf.sesame/sesame-runtime "2.8.11"]
                 [org.slf4j/slf4j-log4j12 "1.7.9" :exclusions [log4j org.slf4j/slf4j-api]]
                 [prismatic/schema "1.0.4"]
                 [ring "1.4.0" :exclusions [org.clojure/java.classpath]]
                 [ring-middleware-accept "2.0.3"]
                 [ring-server "0.4.0"]
                 [ring/ring-core "1.4.0"]
                 [selmer "0.6.9"]
                 [swirrl/lib-swirrl-server "0.6.3" :exclusions [clout org.clojure/java.classpath]]
                 [wrap-verbs "0.1.1"]]

  ;; Ensure we build the java sub project source code too!
  :java-source-paths ["src-java/drafter_sparql_repository/src/main/java"]

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

  :uberjar-name "drafter.jar"

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


   :perforate { :dependencies [[clj-http "1.1.0"]
                               [criterium "0.4.3"] ;; for easy benchmarking
                               [drafter-client "0.3.6-SNAPSHOT"]
                               [grafter "0.6.0-alpha5"]
                               [perforate "0.3.4"] ;; include perforate and criterium in repl environments
                               ]}

   :dev [:dev-common :dev-overrides]

   :dev-common {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                          [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]]

                :source-paths ["dev/clj"]
                :resource-paths ["dev/resources"]

                :dependencies [ ;;[clj-http "1.1.0"]
                               [clojure-csv/clojure-csv "2.0.1"]
                               [com.aphyr/prism "0.1.1" :exclusions [org.clojure/clojure]]
                               ;;[criterium "0.4.3"] ;; for easy benchmarking
                               ;;[drafter-client "0.3.6-SNAPSHOT"]
                               [org.clojure/data.json "0.2.5"]
                               ;;[perforate "0.3.4"] ;; include perforate and criterium in repl environments
                               [prismatic/schema "1.0.4"]
                               [ring-mock "0.1.5"]
                               [ring/ring-devel "1.3.2" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]]

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
                  ])
