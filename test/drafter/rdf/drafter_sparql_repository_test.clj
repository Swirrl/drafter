(ns drafter.rdf.drafter-sparql-repository-test
  (:require [ring.server.standalone :as ring-server]
            [clojure.test :refer :all]
            [grafter.rdf.repository :as repo]
            [ring.middleware.params :refer [wrap-params]])
  (:import [drafter.rdf DrafterSPARQLRepository]
           [org.openrdf.repository RepositoryException]
           [org.openrdf.query QueryEvaluationException QueryInterruptedException]
           (java.net URI)))

(defn query-timeout-handler
  "Handler which always returns a query timeout response in the format used by Stardog"
  [req]
  {:status 500
   :headers {"SD-Error-Code" "QueryEval"}
   :body "com.complexible.stardog.plan.eval.operator.OperatorException: Query execution cancelled: Execution time exceeded query timeout"})

(def ^:private test-port 8080)

(defn- get-test-repo []
  (let [uri (URI. "http" nil "localhost" test-port nil nil nil)
        repo (DrafterSPARQLRepository. (str uri))]
    (.initialize repo)
    repo))

(defmacro with-server [handler & body]
  `(let [server# (ring-server/serve ~handler {:port test-port :join? false :open-browser? false})]
     (try
       ~@body
       (finally
         (.stop server#)
         (.join server#)))))

(defmacro ignore-exceptions [& body]
  `(try
     ~@body
     (catch Throwable ~'ex nil)))

(defn- extract-query-params-handler [params-ref]
  (wrap-params
    (fn [{:keys [query-params] :as req}]
      (reset! params-ref query-params)
      {:status 200 :headers {} :body ""})))

(deftest query-timeout-test
  (testing "Raises QueryInterruptedException on timeout response"
    (let [repo (get-test-repo)]
      (with-server query-timeout-handler
                   (is (thrown? QueryInterruptedException (repo/query repo "SELECT * WHERE { ?s ?p ?o }"))))))

  (testing "sends timeout header when maxExecutionTime set"
    (let [query-params (atom nil)
          repo (get-test-repo)]
      (with-server (extract-query-params-handler query-params)
                   (let [pquery (repo/prepare-query repo "SELECT * WHERE { ?s ?p ?o }")]
                     (.setMaxExecutionTime pquery 2)
                     (ignore-exceptions (.evaluate pquery))
                     (is (= "2000" (get @query-params "timeout")))))))

  (testing "does not send timeout header when maxExecutionTime not set"
    (let [query-params (atom nil)
          repo (get-test-repo)]
      (with-server (extract-query-params-handler query-params)
                   (ignore-exceptions (repo/query repo "SELECT * WHERE { ?s ?p ?o }"))
                   (is (= false (contains? @query-params "timeout")))))))

