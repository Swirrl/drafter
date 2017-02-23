(ns drafter.rdf.drafter-sparql-repository-test
  (:require [ring.server.standalone :as ring-server]
            [clojure.test :refer :all]
            [grafter.rdf.repository :as repo])
  (:import [drafter.rdf DrafterSPARQLRepository]
           [org.openrdf.repository RepositoryException]
           [org.openrdf.query QueryEvaluationException QueryInterruptedException]))

(defn query-timeout-handler
  "Handler which always returns a query timeout response in the format used by Stardog"
  [req]
  {:status 500
   :headers {"SD-Error-Code" "QueryEval"}
   :body "com.complexible.stardog.plan.eval.operator.OperatorException: Query execution cancelled: Execution time exceeded query timeout"})

(deftest query-timeout-test
  (let [repo (doto (DrafterSPARQLRepository. "http://localhost:8080") (.initialize))
        server (ring-server/serve query-timeout-handler {:port 8080 :join? false :open-browser? false})]
    (try
      (is (thrown? QueryInterruptedException (repo/query repo "SELECT * WHERE { ?s ?p ?o }")))
      (finally
        (.stop server)
        (.join server)))))

