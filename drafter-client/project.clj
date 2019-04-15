(defproject drafter-client "1.0.1-SNAPSHOT"
  :description "Client for the Drafter HTTP API"
  :url "http://github.com/swirrl/drafter-client"
  :source-paths ["src" "generated/src"]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-http "3.9.0"]
                 [cheshire "5.8.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [integrant "0.6.3"]
                 [grafter "0.11.2"]
                 [grafter/vocabularies "0.2.3"]
                 [grafter/url "0.2.5"]
                 [buddy/buddy-sign "3.0.0"]
                 [ring/ring-core "1.6.3"]]
  :profiles
  {:dev {:dependencies [[clj-http-fake "1.0.3"]
                        [org.slf4j/slf4j-log4j12 "1.7.25"]
                        [integrant/repl "0.3.1"]
                        [environ "1.0.3"]]
         :source-paths ["env/dev/clj"]
         :resource-paths ["env/dev/resources"]
         :plugins [[lein-environ "1.0.2"]]}
   :swagger {:dependencies [[io.swagger/swagger-codegen-cli "2.3.1"]]}
   :test {}}
  :aliases {"swagger" ["with-profiles" "swagger"
                       "run" "-m" "io.swagger.codegen.SwaggerCodegen"
                       "generate"
                       "--lang" "clojure"
                       "--input-spec" "resources/drafter.yml"
                       "-o" "generated/"
                       "--additional-properties" "baseNamespace=drafter-swagger-client"]})
