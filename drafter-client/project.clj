(defproject drafter-client "1.0.1-SNAPSHOT"
  :description "Client for the Drafter HTTP API"
  :url "http://github.com/swirrl/drafter-client"
  :source-paths ["src" "generated/src"]
  :dependencies [[buddy/buddy-sign "3.0.0"]
                 [cheshire "5.8.0"]
                 [clj-http "3.9.0"]
                 [grafter "2.0.1"]
                 [grafter/url "0.2.5"]
                 [grafter/vocabularies "0.2.6"]
                 [integrant "0.6.3"]
                 [martian "0.1.10-SNAPSHOT"]
                 [martian-clj-http "0.1.10-SNAPSHOT"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [ring/ring-core "1.6.3"]]
  :profiles
  {:dev {:dependencies [[environ "1.0.3"]
                        [integrant/repl "0.3.1"]
                        [org.slf4j/slf4j-log4j12 "1.7.25"]]
         :source-paths ["env/dev/clj"]
         :resource-paths ["env/dev/resources"]
         :plugins [[lein-environ "1.0.2"]]}})
