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
                                                                         org.apache.httpcomponents/httpclient
                                                                         org.slf4j/slf4j-api]]

                 [org.apache.jena/jena-core "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-base "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]
                 [org.apache.jena/jena-iri "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api]]

                 [org.clojure/clojure "1.7.0"]
                 [me.raynes/fs "1.4.6"] ; ;filesystem utils
                 [lib-noir "0.9.9" :exclusions [compojure org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [ring "1.4.0" :exclusions [org.clojure/java.classpath]]
                 [ring/ring-core "1.4.0"]
                 [ring-server "0.4.0"]
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
                 [grafter "0.6.0-alpha5" :exclusions [org.openrdf.sesame/sesame-queryresultio-sparqlxml org.clojure/tools.nrepl]]
                 [org.openrdf.sesame/sesame-queryresultio-sparqlxml "2.7.16-with-ses-memfix" :exclusions [org.openrdf.sesame/sesame-rio-api]]

                 [grafter/vocabularies "0.1.3"]
                 [grafter/url "0.2.1"]

                 [org.openrdf.sesame/sesame-queryrender "2.7.16"]
                 [org.openrdf.sesame/sesame-runtime "2.7.16"]

                 [com.taoensso/timbre "4.0.2"]
                 [clj-logging-config "1.9.12"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [org.slf4j/slf4j-log4j12 "1.7.9" :exclusions [log4j org.slf4j/slf4j-api]]
                 [ring-middleware-accept "2.0.3"]
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

  :perforate {:environments [{:name :stardog
                              :profiles [:bench-stardog]
                              :namespaces [drafter.publishing-api-benchmarks]
                              :fixtures [drafter.publishing-api-benchmarks/wrap-with-database]}

                             {:name :sesame
                              :profiles [:bench-sesame]
                              :namespaces [drafter.publishing-api-benchmarks]}] }

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
                        [ring/ring-devel "1.3.2" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]
                        [perforate "0.3.4"] ;; include perforate in repl environments for easy benchmarking
                        [criterium "0.4.3"] ;; Update criterium included in perforate to include new bug fixes
                        ;;[clj-http "1.1.0"]
                        [drafter-client "0.3.6-SNAPSHOT"]]

         :env {:dev true}

         ;:jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"]
         ;;:jvm-opts ["-Djava.awt.headless=true" "-XX:+UnlockCommercialFeatures"  "-XX:+FlightRecorder" "-XX:FlightRecorderOptions=defaultrecording=true,disk=true"]
         }

   ;; Create stardog DB with the following command first:
   ;;
   ;; ./stardog-admin db create -n pmd-benchmark -t D -o strict.parsing=false -o query.all.graphs=true -o reasoning.type=none -o index.differential.enable.limit=0 -o index.differential.merge.limit=20000
   :bench-stardog {:env {:sparql-query-endpoint "http://localhost:5820/pmd-benchmark/query"
                         :sparql-update-endpoint "http://localhost:5820/pmd-benchmark/update"
                         :drafter-backend "drafter.backend.stardog.sesame"}}

   :bench-sesame {:env {:default-repo-path "drafter-bench-db"
                        :drafter-backend "drafter.backend.sesame.native"}}
   }


  :jvm-opts ["-Djava.awt.headless=true"]

  ;NOTE: expected JVM version to run against is defined in the Dockerfile
  :javac-options ["-target" "7" "-source" "7"]
  :min-lein-version "2.5.0"
  )
