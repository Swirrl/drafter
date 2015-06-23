(defproject drafter "0.3.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://github.com/Swirrl/drafter"
  :license {:name "Proprietary & Commercially Licensed Only"
            :url "http://swirrl.com/"}

  ;; SNAPSHOT repository for JENA builds
  :repositories [["jena-snapshots" {:url "https://repository.apache.org/content/repositories/snapshots/"
                                    :releases false}]]

  :dependencies [
                 ;; Lock to a snapshot release of 3.0.0 as it has the JENA-954
                 ;; bug fix that we depend upon for rewriting.
                 ;;
                 ;; TODO: Update to JENA 3.0.0 when it is released.
                 [org.apache.jena/jena-arq "3.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-api
                                                                                   com.fasterxml.jackson.core/jackson-core
                                                                                   org.slf4j/jcl-over-slf4j
                                                                                   org.slf4j/slf4j-api]]

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
                 [grafter "0.5.0-SNAPSHOT" :exclusions [org.openrdf.sesame/sesame-runtime org.clojure/tools.reader]]
                 [grafter/vocabularies "0.1.0"]

                 [org.openrdf.sesame/sesame-queryrender "2.7.14"]
                 [org.openrdf.sesame/sesame-runtime "2.7.14"]



                 [clj-logging-config "1.9.12"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [org.slf4j/slf4j-log4j12 "1.7.9" :exclusions [log4j org.slf4j/slf4j-api]]
                 [ring-middleware-accept "2.0.2"]
                 [environ "1.0.0"]]

  :java-source-paths ["src-java"]
  :resource-paths ["resources"]
  :pedantic? :abort

  :repl-options {:init-ns drafter.repl
                 :init (-main)}

  :plugins [[lein-ring "0.8.10" :exclusions [org.clojure/clojure]]
            [lein-environ "1.0.0"]
            [lein-test-out "0.3.1" :exclusions [org.clojure/tools.namespace]]]

  :ring {:handler drafter.handler/app
         :init    drafter.handler/init
         :destroy drafter.handler/destroy}

  :aliases {"reindex" ["run" "-m" "drafter.handler/reindex"]}

  :target-path "target/%s" ;; ensure profiles don't pollute each other with
                           ;; compiled classes etc...

  :clean-targets [:target-path :compile-path]

  :profiles
  {
   :uberjar {:aot :all
             :main drafter.repl}

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


  :jvm-opts ["-Djava.awt.headless=true"]

  ;NOTE: expected JVM version to run against is defined in the Dockerfile
  :javac-options ["-target" "7" "-source" "7"]
  :min-lein-version "2.5.0"
  )
