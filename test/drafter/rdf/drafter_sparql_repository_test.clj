(ns drafter.rdf.drafter-sparql-repository-test
  (:require [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [grafter.rdf.repository :as repo])
  (:import drafter.rdf.DrafterSparqlSession
           java.io.ByteArrayOutputStream
           [java.util.concurrent CountDownLatch ExecutionException TimeUnit]
           org.openrdf.query.QueryInterruptedException))

(defn query-timeout-handler
  "Handler which always returns a query timeout response in the format used by Stardog"
  [req]
  {:status 500
   :headers {"SD-Error-Code" "QueryEval"}
   :body "com.complexible.stardog.plan.eval.operator.OperatorException: Query execution cancelled: Execution time exceeded query timeout"})

(def ^:private test-port 8080)

(deftest query-timeout-test
  (testing "Raises QueryInterruptedException on timeout response"
    (let [repo (tc/get-latched-http-server-repo test-port)]
      (tc/with-server test-port query-timeout-handler
                   (is (thrown? QueryInterruptedException (repo/query repo "SELECT * WHERE { ?s ?p ?o }"))))))

  (testing "sends timeout header when maxExecutionTime set"
    (let [query-params (atom nil)
          repo (tc/get-latched-http-server-repo test-port)]
      (tc/with-server test-port (tc/extract-query-params-handler query-params tc/ok-spo-query-response)
                   (let [pquery (repo/prepare-query repo "SELECT * WHERE { ?s ?p ?o }")]
                     (.setMaxExecutionTime pquery 2)
                     (.evaluate pquery)
                     (is (= "2000" (get @query-params "timeout")))))))

  (testing "does not send timeout header when maxExecutionTime not set"
    (let [query-params (atom nil)
          repo (tc/get-latched-http-server-repo test-port)]
      (tc/with-server test-port (tc/extract-query-params-handler query-params tc/ok-spo-query-response)
                   (repo/query repo "SELECT * WHERE { ?s ?p ?o }")
                   (is (= false (contains? @query-params "timeout")))))))

(deftest query-method-test
  (testing "short query should be GET request"
    (let [method (atom nil)
          repo (tc/get-latched-http-server-repo test-port)]
      (tc/with-server test-port (tc/extract-method-handler method tc/ok-spo-query-response)
                   (repo/query repo "SELECT * WHERE { ?s ?p ?o }")
                   (is (= :get @method)))))

  (testing "long query should be POST request"
    (let [uris (map #(str "http://s/" (inc %)) (range))
          bindings (map #(format "<%s>" %) uris)
          sb (StringBuilder. "SELECT * WHERE { VALUES ?u { ")
          method (atom nil)
          repo (tc/get-latched-http-server-repo test-port)]
      (loop [s bindings]
        (when (< (.length sb) DrafterSparqlSession/STARDOG_MAXIMUM_URL_LENGTH)
          (.append sb (first s))
          (.append sb " ")
          (recur (rest bindings))))
      (.append sb "} ?u ?p ?o }")
      (tc/with-server test-port (tc/extract-method-handler method tc/ok-spo-query-response)
                   (repo/query repo (str sb))
                   (is (= :post @method))))))

(defn- http-response->string [response]
  (let [baos (ByteArrayOutputStream. 1024)]
    (tc/write-response response baos)
    (String. (.toByteArray baos))))

(defn- make-blocking-connection [repo idx]
  (future
    (repo/query repo "SELECT * WHERE { ?s ?p ?o }")
    (keyword (str "future-" idx))))

(deftest connection-timeout
  (testing "Connection timeout"
    (let [max-connections (int 2)
          connection-latch (CountDownLatch. max-connections)
          release-latch (CountDownLatch. 1)
          repo (doto (tc/get-latched-http-server-repo test-port) (.setMaxConcurrentHttpConnections max-connections))]
      (with-open [server (tc/latched-http-server test-port connection-latch release-latch (tc/get-spo-http-response))]
        (let [blocked-connections (doall (map #(make-blocking-connection repo %) (range 1 (inc max-connections))))]
          ;;wait for max number of connections to be accepted by the server
          (if (.await connection-latch 5000 TimeUnit/MILLISECONDS)
            (do
              ;;server has accepted max number of connections so next query attempt should see a connection timeout
              (let [rf (future
                         (repo/query repo "SELECT * WHERE { ?s ?p ?o }"))]
                ;;should be rejected almost immediately
                (try
                  (.get rf 5000 TimeUnit/MILLISECONDS)
                  (catch ExecutionException ex
                    (is (instance? QueryInterruptedException (.getCause ex))))
                  (catch Throwable ex
                    (is false "Expected query to be rejected due to timeout"))))

              ;;release previous connections and wait for them to complete
              (.countDown release-latch)
              (doseq [f blocked-connections]
                (.get f 1000 TimeUnit/MILLISECONDS)))
            (throw (RuntimeException. "Server failed to accept connections within timeout"))))))))
