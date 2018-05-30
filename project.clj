(def VERSION (or (System/getenv "TRAVIS_TAG") "local-2.1.x-SNAPSHOT"))

(defproject drafter VERSION
  :description "Backend PMD service"
  :url "http://github.com/Swirrl/drafter"
  :license {:name "Proprietary & Commercially Licensed Only"
            :url "http://swirrl.com/"}

  ;; :repositories [["snapshots" {:url "s3p://swirrl-jars/snapshots/"
  ;;                              :sign-releases false
  ;;                              :username :env
  ;;                              :passphrase :env
  ;;                              :releases false}]
  ;;                ["releases" {:url "s3p://swirrl-jars/releases/"
  ;;                             :sign-releases true
  ;;                             :username :env
  ;;                             :passphrase :env
  ;;                             :snapshots false}]]

  :repositories [["apache-dev" {:url "https://repository.apache.org/content/repositories/snapshots/"
                                :releases false}]
                 
                 ]
  
  :classifiers {:prod :prod
                :dev :dev}

  :dependencies [[buddy/buddy-auth "2.1.0"]
                 [buddy/buddy-core "1.4.0"]

                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/spec.alpha "0.1.134"]
                 
                 [org.clojure/math.combinatorics "0.1.4"]
                 
                 [cognician/dogstatsd-clj "0.1.2"]

                 [commons-codec "1.11"]
                 
                 [clj-yaml "0.4.0"] ;; for loading our Swagger schemas
                 [metosin/scjsv "0.4.0"] ;; for validating our Swagger/JSON schemas

                 [aero "1.1.2"]

                 [integrant "0.6.3"]
                 
                 ;; Lock dependency of jackson to a version that
                 ;; works with sesame's sparql json results renderer
                 ;; and the scjsv json schema validator.
                 ;;
                 ;; NOTE: When we upgrade sesame to RDF4j we can possibly
                 ;; drop this override.
                 ;;
                 ;; Without this you get errors like:
                 ;; java.lang.NoClassDefFoundError: com/fasterxml/jackson/core/FormatFeature, compiling:(cheshire/factory.clj:54:7)
                 [com.fasterxml.jackson.core/jackson-core "2.6.7"]
                 
                 [com.novemberain/monger "3.1.0"]

                 [com.sun.mail/javax.mail "1.6.0"]
                 ;;[com.taoensso/tower "2.0.2"]

                 [grafter "0.11.0-drafter-rdf4j-261-SNAPSHOT"]

                 [grafter/url "0.2.5"]
                 ;[grafter/vocabularies "0.1.3"]
                 [lib-noir "0.9.9" :exclusions [compojure org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [me.raynes/fs "1.4.6"] ;; filesystem utils

                 [metosin/ring-swagger-ui "2.2.10"]

                 ;; Use JENA for our query rewriting
                 ;; Use private release of Jena based on commit 44b478ea8637ca16035e56ef49d6cb6e59abc289
                 ;; See https://github.com/Swirrl/drafter/issues/270
                 ;; This should be updated to use 3.9.0 when that version is released
                 [org.apache.jena/jena-arq "3.9.0-44b478ea" :exclusions [org.slf4j/slf4j-api
                                                                         org.slf4j/jcl-over-slf4j
                                                                         org.apache.httpcomponents/httpclient]]
                 [org.apache.jena/jena-base "3.9.0-44b478ea" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-core "3.9.0-44b478ea" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-iri "3.9.0-44b478ea" :exclusions [org.slf4j/slf4j-api]]
                 

                 [org.mindrot/jbcrypt "0.4"]

                 [org.slf4j/slf4j-log4j12 "1.7.25" :exclusions [log4j org.slf4j/slf4j-api]]
                 [prismatic/schema "1.1.7"]

                 [ring-middleware-format "0.7.2"]
                 [ring "1.6.2" :exclusions [org.clojure/java.classpath]]
                 [ring-middleware-accept "2.0.3"]
                 [ring-server "0.5.0"]
                 [ring/ring-core "1.6.2"]
                 ;;[selmer "0.6.9"]
                 [swirrl/lib-swirrl-server "0.6.3" :exclusions [clout org.clojure/java.classpath]]
                 [wrap-verbs "0.1.1"]]

  ;; Ensure we build the java sub project source code too!
  :java-source-paths ["src-java/drafter_sparql_repository/src/main/java"]

  :resource-paths ["resources"]
  :pedantic :abort

  ;;:target-path "target/%s" ;; ensure profiles don't pollute each other with
  ;; compiled classes etc...

  :clean-targets [:target-path :compile-path]

  :profiles
  {
   :java9 {:jvm-opts ["--add-modules" "java.xml.bind"]}
   
   :uberjar [:prod
             {:aot :all
              :main drafter.main}]

   :prod {:uberjar-name "drafter.jar"
          :source-paths ["env/prod/clj"]
          :resource-paths ["env/prod/resources"]}

   :dev [:dev-common]

   :dev-common {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                          [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]]

                :repl-options {:init-ns user
                               :timeout 180000}
                
                :source-paths ["env/dev/clj"]
                :resource-paths ["env/dev/resources" "test/resources"]

                :dependencies [[clojure-csv/clojure-csv "2.0.2"]
                               [org.clojure/data.json "0.2.6"]
                               [ring-mock "0.1.5"]
                               [ring/ring-devel "1.6.2" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]
                               [eftest "0.3.2"] ;; repl test runner support
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

  ;; Target JDK 8 expected JVM version
  :javac-options ["-target" "8" "-source" "8"]
  :min-lein-version "2.8.1"
  

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ;;["vcs" "push"]
                  ])
