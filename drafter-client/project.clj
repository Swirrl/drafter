(defproject drafter-client "1.0.1-SNAPSHOT"
  :description "Client for the Drafter HTTP API"
  :url "http://github.com/swirrl/drafter-client"
  :source-paths ["src" "generated/src"]
  :dependencies [[buddy/buddy-sign "3.4.1"]
                 [cheshire "5.10.1"]
                 [clj-http "3.12.3"]
                 [grafter "2.1.17"]
                 [grafter/url "0.2.5"]
                 [grafter/vocabularies "0.3.8"]
                 [integrant "0.8.0"]
                 [martian "0.1.16"]
                 [martian-clj-http "0.1.16"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.logging "1.2.3"]
                 [ring/ring-core "1.9.4"]]
  :profiles
  {:dev {:dependencies [[environ "1.2.0"]
                        [integrant/repl "0.3.2"]
                        [org.slf4j/slf4j-log4j12 "1.7.32"]]
         :source-paths ["env/dev/clj"]
         :resource-paths ["env/dev/resources"]
         :plugins [[lein-environ "1.2.0"]]}})
