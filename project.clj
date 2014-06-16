(defproject drafter "0.1.0-SNAPSHOT"
  :description "Backend PMD service"
  :url "http://example.com/FIXME"
  :repositories [["apache" "https://repository.apache.org/content/repositories/releases/"]]

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.jena/jena-core "2.11.2"]
                 [org.apache.jena/jena-arq "2.11.2"]
                 [org.apache.jena/jena-tdb "1.0.2"]
                 [org.apache.jena/jena-iri "1.0.2"]

                 [lib-noir "0.8.3"]
                 [ring-server "0.3.1"]
                 [selmer "0.6.6"]
                 [com.taoensso/timbre "3.2.1"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [environ "0.5.0"]]

  :repl-options {:init-ns drafter.repl}
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
   :dev {:dependencies [[ring-mock "0.1.5"]
                        [ring/ring-devel "1.2.2"]]
         :env {:dev true}}}
  :min-lein-version "2.0.0"
  )
