(ns drafter.test-common
  (:require [clojure.test :refer :all]
            [drafter
             [draftset :refer [->draftset-uri]]
             [user :as user]
             [write-scheduler :refer [global-writes-lock queue-job! start-writer! stop-writer!]]]
            [drafter.backend.protocols :refer [stop-backend ->repo-connection]]
            [drafter.backend.sesame.remote :refer [get-backend]]
            [drafter.rdf
             [draft-management :refer [create-draft-graph! create-managed-graph! migrate-graphs-to-live!]]
             [sparql :as sparql]]
            [environ.core :refer [env]]
            [grafter.rdf
             [protocols :refer [add]]
             [templater :refer [triplify]]]
            [grafter.rdf.repository.registry :as reg]
            [ring.middleware.params :refer [wrap-params]]
            [ring.server.standalone :as ring-server]
            [schema
             [core :as s]
             [test :refer [validate-schemas]]]
            [swirrl-server.async.jobs :refer [create-job]]
            [grafter.rdf.repository :as repo])
  (:import drafter.rdf.DrafterSPARQLRepository
           [java.io ByteArrayInputStream ByteArrayOutputStream OutputStream PrintWriter]
           java.lang.AutoCloseable
           [java.net InetSocketAddress ServerSocket SocketException URI]
           java.nio.charset.Charset
           [java.util ArrayList Scanner UUID]
           [java.util.concurrent CountDownLatch TimeUnit]
           [org.apache.http.entity ContentLengthStrategy ContentType StringEntity]
           org.apache.http.impl.entity.StrictContentLengthStrategy
           [org.apache.http.impl.io ChunkedOutputStream ContentLengthOutputStream DefaultHttpRequestParser DefaultHttpResponseWriter HttpTransportMetricsImpl IdentityOutputStream SessionInputBufferImpl SessionOutputBufferImpl]
           org.apache.http.message.BasicHttpResponse
           org.apache.http.ProtocolVersion
           org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter
           org.openrdf.rio.trig.TriGParserFactory))

(use-fixtures :each validate-schemas)

(def ^:dynamic *test-backend*)

(defn test-triples [subject-uri]
  (triplify [subject-uri
             [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/1")]
             [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/2")]]))

(defn select-all-in-graph [graph-uri]
  (str "SELECT * WHERE {"
       "   GRAPH <" graph-uri "> {"
       "     ?s ?p ?o ."
       "   }"
       "}"))

(defn stream->string
  "Hack to convert a stream to a string."
  ;; http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string
  [stream]
  (let [scanner (doto (Scanner. stream)
                  (.useDelimiter "\\A"))]
    (if (-> scanner .hasNext)
      (.next scanner)
      "")))

(defn with-identity
  "Sets the given test user as the user on a request"
  [user request]
  (let [unencoded-auth (str (user/username user) ":" "password")
        encoded-auth (buddy.core.codecs/str->base64 unencoded-auth)]
    (-> request
        (assoc :identity user)
        (assoc-in [:headers "Authorization"] (str "Basic " encoded-auth)))))

(defn wait-for-lock-ms [lock period-ms]
  (if (.tryLock lock period-ms (TimeUnit/MILLISECONDS))
    (.unlock lock)
    (throw (RuntimeException. (str "Lock not released after " period-ms "ms")))))

(declare ^:dynamic *test-writer*)

(defn wrap-db-setup [test-fn]
  (let [backend (get-backend env)
        configured-factories (reg/registered-parser-factories)]
    (binding [*test-backend* backend
              *test-writer* (start-writer!)]
      (do
        ; Some tests need to load and parse trig file data
        (reg/register-parser-factory! :construct TriGParserFactory)
        (try
          (test-fn)
          (finally
            (stop-backend backend)
            (stop-writer! *test-writer*)
            (reg/register-parser-factories! configured-factories)))))))

(defn wrap-clean-test-db
  ([test-fn] (wrap-clean-test-db identity test-fn))
  ([setup-state-fn test-fn]
   (sparql/update! *test-backend*
            "DROP ALL ;")
   (setup-state-fn *test-backend*)
   (test-fn)))

(defn make-backend []
  (get-backend env))

(defn during-exclusive-write-f [f]
  (let [p (promise)
        latch (CountDownLatch. 1)
        exclusive-job (create-job :publish-write
                                  (fn [j]
                                    (.countDown latch)
                                    @p))]

    ;; submit exclusive job which should prevent updates from being
    ;; scheduled
    (queue-job! exclusive-job)

    ;; wait until exclusive job is actually running i.e. the write lock has
    ;; been taken
    (.await latch)

    (try
      (f)
      (finally
        ;; complete exclusive job
        (deliver p nil)

        ;; wait a short time for the lock to be released
        (wait-for-lock-ms global-writes-lock 200)))))

(defmacro during-exclusive-write [& forms]
  `(during-exclusive-write-f (fn [] ~@forms)))

(defmacro throws-exception?
  "Test that the form raises an exception, will cause a test failure if it doesn't.

  Unlike clojure.test/thrown?  It allows you to catch the form and do
  further tests on the exception."
  [form & catch-forms]
  `(try
     ~form
     (is false (str "Expected " (pr-str (quote ~form)) " to raise exception. DONT be confused by the 'false false' test failure here, it failed because no exception was thrown."))
     ~@catch-forms))

(defn ask? [& graphpatterns]
  "Bodgy convenience function for ask queries"
  (repo/query *test-backend* (str "ASK WHERE {"
                        (-> (apply str (interpose " " graphpatterns))
                            (.replace " >" ">")
                            (.replace "< " "<"))
                        "}")))

(def default-timeout 5000)

(def job-id-path #"/v1/status/finished-jobs/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})")

(defn job-path->job-id [job-path]
  (if-let [uid (second (re-matches job-id-path job-path))]
    (UUID/fromString uid)))

(defn await-completion
  "Test helper to await for an async operation to complete.  Takes the
  state atom a GUID for the job id and waits until timeout for the job
  to appear.

  If the job doesn't appear before timeout time has passed an
  exception is raised."
  ([state-atom path] (await-completion state-atom path default-timeout))
  ([state-atom path timeout]
   (let [start (System/currentTimeMillis)]
     (loop [state-atom state-atom
            guid (job-path->job-id path)]
       (if-let [value (@state-atom guid)]
         @value
         (if (> (System/currentTimeMillis) (+ start (or timeout default-timeout) ))
           (throw (RuntimeException. "Timed out awaiting test value"))
           (do
             (Thread/sleep 5)
             (recur state-atom guid))))))))

(defmacro await-success
  "Waits for the job with the given path to be present in the given
  job state atom and then asserts the job succeeded. Returns the job
  result map."
  [state-atom job-path]
  `(let [job-result# (await-completion ~state-atom ~job-path)]
     (is (= :ok (:type job-result#)) (str "job failed: " (:exception job-result#)))
     job-result#))

(defn empty-spo-json-body []
  (let [baos (ByteArrayOutputStream. 1024)
        writer (SPARQLResultsJSONWriter. baos)]
    (.startQueryResult writer (ArrayList. ["s" "p" "o"]))
    (.endQueryResult writer)
    (String. (.toByteArray baos))))

(defn get-spo-http-response []
  (let [response (BasicHttpResponse. (ProtocolVersion. "HTTP" 1 1) 200 "OK")
        content-type (ContentType/create "application/sparql-results+json" (Charset/forName "UTF8"))
        entity (StringEntity. (empty-spo-json-body) content-type)]
    (.addHeader response "Content-Type" "application/sparql-results+json")
    (.setEntity response entity)
    response))

(defn- read-http-request [input-stream]
  (let [metrics (HttpTransportMetricsImpl.)
        buf (SessionInputBufferImpl. metrics (* 8 1024))]
    (.bind buf input-stream)
    (let [parser (DefaultHttpRequestParser. buf)]
      (.parse parser))))

(defn write-response [response output-stream]
  (let [metrics (HttpTransportMetricsImpl.)
        buf (SessionOutputBufferImpl. metrics (* 8 1024))]
    (.bind buf output-stream)
    (let [writer (DefaultHttpResponseWriter. buf)]

      (.write writer response)
      (if-let [entity (.getEntity response)]
        (let [len (.determineLength (StrictContentLengthStrategy.) response)]
          (with-open [os (cond
                           (= len (long ContentLengthStrategy/CHUNKED)) (ChunkedOutputStream. 2048 buf)
                           (= len (long ContentLengthStrategy/IDENTITY)) (IdentityOutputStream. buf)
                           :else (ContentLengthOutputStream. buf len))]

            (.writeTo entity os))))
      (.flush buf))))

(defn- handle-latched-http-client [client-socket connection-latch release-latch response]
  ;;notify connection has been accepted
  (.countDown connection-latch)

  (with-open [socket client-socket]
    ;;read entire request
    (read-http-request (.getInputStream socket))

    ;;wait for signal
    (.await release-latch)

    ;;write response
    (write-response response (.getOutputStream socket))))

(defn- latched-http-handler [port listener connection-latch release-latch response]
  (let [client-threads (atom [])]
    (try
      (.bind listener (InetSocketAddress. port))
      (loop [client-socket (.accept listener)]
        (let [^Runnable client-handler #(handle-latched-http-client client-socket connection-latch release-latch response)
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

(defn latched-http-server [port connection-latch release-latch response]
  (let [listener (ServerSocket.)
        server-fn ^Runnable (fn [] (latched-http-handler port listener connection-latch release-latch response))
        server-thread (Thread. server-fn "Test server thread")]
    (.start server-thread)
    (reify AutoCloseable
      (close [_]
        (.close listener)
        (try
          (.join server-thread 500)
          (catch InterruptedException ex nil))))))

(defn get-latched-http-server-repo
  "Returns a DrafterSPARQLRepository with a query URI matching latched-http-handler
   listening on the given port."
  [port]
  (let [uri (URI. "http" nil "localhost" port nil nil nil)
        repo (DrafterSPARQLRepository. (str uri))]
    (.initialize repo)
    repo))

(defn null-output-stream []
  (proxy [OutputStream] []
    (close [])
    (flush [])
    (write
      ([_])
      ([bytes offset length]))))

(defmacro suppress-stdout [& body]
  `(binding [*out* (PrintWriter. (null-output-stream))]
     ~@body))

(defmacro with-server [port handler & body]
  `(let [server# (suppress-stdout (ring-server/serve ~handler {:port ~port :join? false :open-browser? false}))]
     (try
       ~@body
       (finally
         (.stop server#)
         (.join server#)))))

(def ok-spo-query-response
  {:status 200 :headers {"Content-Type" "application/sparql-results+json"} :body (empty-spo-json-body)})

(defn extract-query-params-handler [params-ref response]
  (wrap-params
    (fn [{:keys [query-params] :as req}]
      (reset! params-ref query-params)
      response)))

(defn extract-method-handler [method-ref response]
  (fn [{:keys [request-method]}]
    (reset! method-ref request-method)
    response))

(defn key-set
  "Gets a set containing the keys in the given map."
  [m]
  (set (keys m)))

(defn assert-schema [schema value]
  (let [errors (s/check schema value)]
    (is (not errors) errors)))

(def ring-response-schema
  {:status s/Int
   :headers {s/Str s/Str}
   :body s/Any})

(defn response-code-schema [code]
  (assoc ring-response-schema :status (s/eq code)))

(defn assert-is-ok-response [response]
  (assert-schema (response-code-schema 200) response))

(defn assert-is-accepted-response [response]
  (assert-schema (response-code-schema 202) response))

(defn assert-is-not-found-response [response]
  (assert-schema (response-code-schema 404) response))

(defn assert-is-not-acceptable-response [response]
  (assert-schema (response-code-schema 406) response))

(defn assert-is-unprocessable-response [response]
  (assert-schema (response-code-schema 422) response))

(defn assert-is-unsupported-media-type-response [response]
  (assert-schema (response-code-schema 415) response))

(defn assert-is-method-not-allowed-response [response]
  (assert-schema (response-code-schema 405) response))

(defn assert-is-forbidden-response [response]
  (assert-schema (response-code-schema 403) response))

(defn assert-is-unauthorised-response [response]
  (assert-schema (response-code-schema 401) response))

(defn assert-is-bad-request-response [response]
  (assert-schema (response-code-schema 400) response))

(defn assert-is-service-unavailable-response [response]
  (assert-schema (response-code-schema 503) response))

(defn string->input-stream [s]
  (ByteArrayInputStream. (.getBytes s)))
