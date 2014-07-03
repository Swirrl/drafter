(defproject drafter "0.1.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://example.com/FIXME"
  :repositories [["apache" "https://repository.apache.org/content/repositories/releases/"]
                 ["swirrl-private" {:url "s3p://leiningen-private-repo/releases/"
                                    :username :env
                                    :passphrase :env}]]

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.jena/jena-core "2.11.2"]
                 [org.apache.jena/jena-arq "2.11.2"]
                 [org.apache.jena/jena-tdb "1.0.2"]
                 [org.apache.jena/jena-iri "1.0.2"]
                 [me.raynes/fs "1.4.4"] ; ;filesystem utils
                 [lib-noir "0.8.4"]
                 [ring-server "0.3.1"]
                 [selmer "0.6.6"]
                 [com.taoensso/timbre "3.2.1"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [grafter "0.2.0-SNAPSHOT"]
                 [environ "0.5.0"]]

  :repl-options {:init-ns drafter.repl
                 :port 5678}
  :plugins [[lein-ring "0.8.10"]
            [lein-environ "0.5.0"]]
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
                        [ring/ring-devel "1.2.2"]]

         :env {:dev true}}}

  :min-lein-version "2.0.0"
  )
