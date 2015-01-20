(defproject drafter "0.1.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://example.com/FIXME"
  ;; :repositories [["apache" "https://repository.apache.org/content/repositories/releases/"]
  ;;                ["swirrl-private" {:url "s3p://leiningen-private-repo/releases/"
  ;;                                   :username :env
  ;;                                   :passphrase :env}]
  ;;                ["swirrl-private-snapshots" {:url "s3p://leiningen-private-repo/snapshots/"
  ;;                                             :username :env
  ;;                                             :passphrase :env}]
  ;;                ]

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [me.raynes/fs "1.4.6"] ; ;filesystem utils
                 [lib-noir "0.8.4" :exclusions [org.clojure/java.classpath org.clojure/tools.reader org.clojure/java.classpath]]
                 [ring-server "0.3.1"]
                 [wrap-verbs "0.1.1"]
                 [selmer "0.6.9"]
                 [grafter "0.3.0-SNAPSHOT" :exclusions [[org.openrdf.sesame/sesame-runtime]]]

                 [org.openrdf.sesame/sesame-queryrender "2.7.14"]
                 [org.openrdf.sesame/sesame-runtime "2.7.14"

                  ;; For some reason there appears to be a weird
                  ;; version conflict with this sesame library.  So
                  ;; exclude it, as we're not using it.

                  :exclusions [org.openrdf.sesame/sesame-repository-manager]]

                 [clj-logging-config "1.9.12"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [org.slf4j/slf4j-log4j12 "1.7.7" :exclusions [log4j org.slf4j/slf4j-api]]
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

  :profiles
  {:uberjar {:aot :all}
   :production {:ring {:open-browser? false
                       :stacktraces?  false
                       :auto-reload?  false}}
   :dev {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                   [s3-wagon-private "1.1.2" :exclusions [commons-logging commons-codec]]]

         :dependencies [[ring-mock "0.1.5"]
                        [com.aphyr/prism "0.1.1" :exclusions [org.clojure/clojure]]
                        [org.clojure/data.json "0.2.5"]
                        [clojure-csv/clojure-csv "2.0.1"]
                        [ring/ring-devel "1.3.0" :exclusions [org.clojure/java.classpath org.clojure/tools.reader]]]

         :env {:dev true}

         ;:jvm-opts ["-Djava.awt.headless=true" "-XX:+UnlockCommercialFeatures"  "-XX:+FlightRecorder" "-XX:FlightRecorderOptions=defaultrecording=true,disk=true"]
         }
   }


  :jvm-opts ["-Djava.awt.headless=true"]
  :min-lein-version "2.5.0"

  :aot [drafter.repl]
  :main drafter.repl
  )
