(ns drafter.rdf.drafter-sparql-repository-test
  (:require [ring.server.standalone :as ring-server]
            [clojure.test :refer :all]
            [grafter.rdf.repository :as repo]
            [ring.middleware.params :refer [wrap-params]]
            [grafter.sequences :refer [alphabetical-column-names]]
            [drafter.test-common :as tc])
  (:import [drafter.rdf DrafterSPARQLRepository DrafterSparqlSession]
           [org.openrdf.query QueryEvaluationException QueryInterruptedException]
           (java.net URI ServerSocket InetSocketAddress SocketException)
           (java.util.concurrent CountDownLatch TimeUnit ExecutionException)
           (org.apache.http.impl.io HttpTransportMetricsImpl SessionInputBufferImpl DefaultHttpRequestParser SessionOutputBufferImpl DefaultHttpResponseWriter ChunkedOutputStream IdentityOutputStream ContentLengthOutputStream)
           (org.apache.http.message BasicHttpResponse)
           (org.apache.http ProtocolVersion)
           (org.openrdf.query.resultio.sparqljson SPARQLResultsJSONWriter)
           (java.io ByteArrayOutputStream PrintWriter OutputStream)
           (java.util ArrayList)
           (org.apache.http.entity StringEntity ContentType ContentLengthStrategy)
           (java.nio.charset Charset)
           (org.apache.http.impl.entity StrictContentLengthStrategy)
           (java.lang AutoCloseable)))

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

(defn- null-output-stream []
  (proxy [OutputStream] []
         (close [])
         (flush [])
         (write
           ([_])
           ([bytes offset length]))))

(defmacro suppress-stdout [& body]
  `(binding [*out* (PrintWriter. (null-output-stream))]
     ~@body))

(defmacro with-server [handler & body]
  `(let [server# (suppress-stdout (ring-server/serve ~handler {:port test-port :join? false :open-browser? false}))]
     (try
       ~@body
       (finally
         (.stop server#)
         (.join server#)))))

(def ok-spo-query-response
  {:status 200 :headers {"Content-Type" "application/sparql-results+json"} :body (tc/empty-spo-json-body)})

(defn- extract-query-params-handler [params-ref]
  (wrap-params
    (fn [{:keys [query-params] :as req}]
      (reset! params-ref query-params)
      ok-spo-query-response)))

(defn- extract-method-handler [method-ref]
  (fn [{:keys [request-method]}]
    (reset! method-ref request-method)
    ok-spo-query-response))

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
                     (.evaluate pquery)
                     (is (= "2000" (get @query-params "timeout")))))))

  (testing "does not send timeout header when maxExecutionTime not set"
    (let [query-params (atom nil)
          repo (get-test-repo)]
      (with-server (extract-query-params-handler query-params)
                   (repo/query repo "SELECT * WHERE { ?s ?p ?o }")
                   (is (= false (contains? @query-params "timeout")))))))

(deftest query-method-test
  (testing "short query should be GET request"
    (let [method (atom nil)
          repo (get-test-repo)]
      (with-server (extract-method-handler method)
                   (repo/query repo "SELECT * WHERE { ?s ?p ?o }")
                   (is (= :get @method)))))

  (testing "long query should be POST request"
    (let [uris (map #(str "http://s/" (inc %)) (range))
          bindings (map #(format "<%s>" %) uris)
          sb (StringBuilder. "SELECT * WHERE { VALUES ?u { ")
          method (atom nil)
          repo (get-test-repo)]
      (loop [s bindings]
        (when (< (.length sb) DrafterSparqlSession/STARDOG_MAXIMUM_URL_LENGTH)
          (.append sb (first s))
          (.append sb " ")
          (recur (rest bindings))))
      (.append sb "} ?u ?p ?o }")
      (with-server (extract-method-handler method)
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
          repo (doto (get-test-repo) (.setMaxConcurrentHttpConnections max-connections))]
      (with-open [server (tc/latched-http-handler test-port connection-latch release-latch (tc/get-spo-http-response))]
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
