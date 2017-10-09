(defproject drafter "2.1.5-SNAPSHOT"
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

  :classifiers {:prod :prod
                :dev :dev}

  :dependencies [[buddy/buddy-auth "0.9.0"]
                 [buddy/buddy-core "0.9.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.openrdf.sesame/sesame-queryrender "2.8.11"]
                 [org.openrdf.sesame/sesame-runtime "2.8.11"]
                 [org.openrdf.sesame/sesame-queryresultio-sparqlxml "2.8.11"]

                 [cognician/dogstatsd-clj "0.1.1"]
                 
                 [clj-yaml "0.4.0"] ;; for loading our Swagger schemas
                 [metosin/scjsv "0.3.0"] ;; for validating our Swagger/JSON schemas

                 [aero "1.1.2"]

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
                 [grafter "0.8.8"]
                 [grafter/url "0.2.1"]
                 ;[grafter/vocabularies "0.1.3"]
                 [lib-noir "0.9.9" :exclusions [compojure org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [me.raynes/fs "1.4.6"] ;; filesystem utils
                 [metosin/ring-swagger-ui "2.2.10"]
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
            [lein-test-out "0.3.1" :exclusions [org.clojure/tools.namespace]]]

  :ring {:handler drafter.handler/app
         :init    drafter.handler/init
         :destroy drafter.handler/destroy
         :open-browser? false }

  ;;:target-path "target/%s" ;; ensure profiles don't pollute each other with
  ;; compiled classes etc...

  :clean-targets [:target-path :compile-path]

  :profiles
  {

   :uberjar [:prod
             {:aot :all
              :main drafter.repl}]

   :prod {:uberjar-name "drafter.jar"
          :source-paths ["env/prod/clj"]
          :resource-paths ["env/prod/resources"]}

   :dev [:dev-common :dev-overrides]

   :dev-common {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                          [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]]

                :source-paths ["env/dev/clj"]
                :resource-paths ["env/dev/resources"]

                :dependencies [[clojure-csv/clojure-csv "2.0.1"]
                               [org.clojure/data.json "0.2.5"]
                               [prismatic/schema "1.0.4"]
                               [ring-mock "0.1.5"]
                               [ring/ring-devel "1.3.2" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]
                               [org.clojure/test.check "0.9.0"]]

                ;;:jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]
                ;;:jvm-opts ["-Djava.awt.headless=true" "-XX:+UnlockCommercialFeatures"  "-XX:+FlightRecorder" "-XX:FlightRecorderOptions=defaultrecording=true,disk=true"]
                }
   }

  :jvm-opts ["-Djava.awt.headless=true"
             "-XX:-OmitStackTraceInFastThrow"
             ;; Use this property to control number
             ;; of connections in the SPARQLRepository connection pool:
             ;;
             ;;"-Dhttp.maxConnections=1"
             ]

  ;; Target JDK 7 expected JVM version (though we may now be able to target JDK8 in production)
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
