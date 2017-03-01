(ns drafter.rdf.drafter-sparql-repository-test
  (:require [ring.server.standalone :as ring-server]
            [clojure.test :refer :all]
            [grafter.rdf.repository :as repo]
            [ring.middleware.params :refer [wrap-params]]
            [grafter.sequences :refer [alphabetical-column-names]])
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
           (org.apache.http.impl.entity StrictContentLengthStrategy)))

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

(defn- empty-spo-json-body []
  (let [baos (ByteArrayOutputStream. 1024)
        writer (SPARQLResultsJSONWriter. baos)]
    (.startQueryResult writer (ArrayList. ["s" "p" "o"]))
    (.endQueryResult writer)
    (String. (.toByteArray baos))))

(def ok-spo-query-response
  {:status 200 :headers {"Content-Type" "application/sparql-results+json"} :body (empty-spo-json-body)})

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

(defn- read-http-request [input-stream]
  (let [metrics (HttpTransportMetricsImpl.)
        buf (SessionInputBufferImpl. metrics (* 8 1024))]
    (.bind buf input-stream)
    (let [parser (DefaultHttpRequestParser. buf)]
      (.parse parser))))

(defn- write-spo-http-response [output-stream]
  (let [metrics (HttpTransportMetricsImpl.)
        buf (SessionOutputBufferImpl. metrics (* 8 1024))]
    (.bind buf output-stream)
    (let [writer (DefaultHttpResponseWriter. buf)
          response (BasicHttpResponse. (ProtocolVersion. "HTTP" 1 1) 200 "OK")
          content-type (ContentType/create "application/sparql-results+json" (Charset/forName "UTF8"))
          entity (StringEntity. (empty-spo-json-body) content-type)]
      (.addHeader response "Content-Type" "application/sparql-results+json")
      (.setEntity response entity)

      (let [len (.determineLength (StrictContentLengthStrategy.) response)]
        (with-open [os (cond
                         (= len (long ContentLengthStrategy/CHUNKED)) (ChunkedOutputStream. 2048 buf)
                         (= len (long ContentLengthStrategy/IDENTITY)) (IdentityOutputStream. buf)
                         :else (ContentLengthOutputStream. buf len))]
          (.write writer response)
          (.writeTo entity os)
          (.flush buf))))))

(defn- handle-latched-http-client [client-socket connection-latch release-latch]
  ;;notify connection has been accepted
  (.countDown connection-latch)

  (with-open [socket client-socket]
    ;;read entire request
    (read-http-request (.getInputStream socket))

    ;;wait for signal
    (.await release-latch)

    ;;write response
    (write-spo-http-response (.getOutputStream socket))))

(defn- latched-http-server [listener connection-latch release-latch]
  (let [client-threads (atom [])]
    (try
      (.bind listener (InetSocketAddress. test-port))
      (loop [client-socket (.accept listener)]
        (let [^Runnable client-handler #(handle-latched-http-client client-socket connection-latch release-latch)
              client-thread (Thread. client-handler "Client handler thread")]
          (.start client-thread)
          (swap! client-threads conj client-thread)
          (recur (.accept listener))))
      (catch SocketException ex
        ;;we expect this exception to occur since it is thrown by .accept when the ServerSocket is closed
        )
      (finally
        ;;wait for all clients to complete
        (doseq [ct @client-threads]
          (.join ct 200))))))

(defn- latched-http-handler [connection-latch release-latch]
  (let [listener (ServerSocket.)
        server-fn ^Runnable (fn [] (latched-http-server listener connection-latch release-latch))
        server-thread (Thread. server-fn "Test server thread")]
    (.start server-thread)
    (fn []
      (.close listener)
      (try
        (.join server-thread 500)
        (catch InterruptedException ex nil)))))

(defn- make-blocking-connection [repo idx]
  (future
    (repo/query repo "SELECT * WHERE { ?s ?p ?o }")
    (keyword (str "future-" idx))))

(deftest connection-timeout
  (testing "Connection timeout"
    (try
      (let [max-connections (int 2)
            connection-latch (CountDownLatch. max-connections)
            release-latch (CountDownLatch. 1)
            stop-server-fn (latched-http-handler connection-latch release-latch)
            repo (doto (get-test-repo) (.setMaxConcurrentHttpConnections max-connections))
            blocked-connections (doall (map #(make-blocking-connection repo %) (range 1 (inc max-connections))))]
        (try
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
                    (is (instance? QueryEvaluationException (.getCause ex))))
                  (catch Throwable ex
                    (.printStackTrace ex)
                    (is false "Expected query to be rejected due to timeout"))))

              ;;release previous connections and wait for them to complete
              (.countDown release-latch)
              (doseq [f blocked-connections]
                (.get f 1000 TimeUnit/MILLISECONDS)))
            (throw (RuntimeException. "Server failed to accept connections within timeout")))
          (finally
            (stop-server-fn)))))))
