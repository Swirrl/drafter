(defproject drafter "0.1.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://example.com/FIXME"
  :repositories [["apache" "https://repository.apache.org/content/repositories/releases/"]
                 ;; ["swirrl-private" {:url "s3p://leiningen-private-repo/releases/"
                 ;;                    :username :env
                 ;;                    :passphrase :env}]
                 ["swirrl-private-snapshots" {:url "s3p://leiningen-private-repo/snapshots/"
                                              :username :env
                                              :passphrase :env}]
                 ]

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [me.raynes/fs "1.4.6"] ; ;filesystem utils
                 [lib-noir "0.8.4"]
                 [ring-server "0.3.1"]
                 [selmer "0.6.9"]
                 [grafter "0.2.0-SNAPSHOT" :exclusions [[org.openrdf.sesame/sesame-runtime]]]
                 [org.openrdf.sesame/sesame-queryrender "2.7.13"]
                 ;; 2.7.14-SNAPSHOT contains our fix for https://openrdf.atlassian.net/browse/SES-2111
                 [org.openrdf.sesame/sesame-queryalgebra-model "2.7.14-drafter-patch-SNAPSHOT"]
                 [org.openrdf.sesame/sesame-runtime "2.7.13"

                  ;; For some reason there appears to be a weird
                  ;; version conflict with this sesame library.  So
                  ;; exclude it, as we're not using it.

                  :exclusions [org.openrdf.sesame/sesame-repository-manager]]
                 ;;[org.openrdf.sesame/sesame-runtime "2.8.0-beta2"]
                 ;;[org.openrdf.sesame/sesame-queryrender "2.8.0-beta2"]
                 ;;[com.taoensso/timbre "3.2.1"]
                 ;;[com.palletops/log-config "0.1.4"]   ;; provides make-tools-logging-appender
                 [clj-logging-config "1.9.12"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [org.slf4j/slf4j-log4j12 "1.7.7"]
                 [environ "1.0.0"]]

  :java-source-paths ["src-java"]

  :repl-options {:init-ns drafter.repl
                 :init (-main)
                 :port 5678}

  :plugins [[lein-ring "0.8.10"]
            [lein-environ "1.0.0"]
            [lein-test-out "0.3.1"]]

  :ring {:handler drafter.handler/app
         :init    drafter.handler/init
         :destroy drafter.handler/destroy}

  :profiles
  {:uberjar {:aot :all}
   :production {:ring {:open-browser? false
                       :stacktraces?  false
                       :auto-reload?  false}}
   :dev {:plugins [[com.aphyr/prism "0.1.1"] ;; autotest support simply run: lein prism
                   [s3-wagon-private "1.1.2"]]

         :dependencies [[ring-mock "0.1.5"]
                        [com.aphyr/prism "0.1.1"]
                        [org.clojure/data.json "0.2.5"]
                        [clojure-csv/clojure-csv "2.0.1"]
                        [ring/ring-devel "1.3.0"]]

         :env {:dev true}}}

  ;;:jvm-opts ["-Dorg.openrdf.repository.debug=true"]
  :min-lein-version "2.0.0"

  :aot [drafter.repl]
  :main drafter.repl
  )
