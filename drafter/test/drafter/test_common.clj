(ns drafter.test-common
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [drafter.backend.common :refer [stop-backend]]
            [drafter.backend.draftset.draft-management
             :refer
             [create-draft-graph! create-managed-graph! migrate-graphs-to-live!]]
            [drafter.configuration :refer [get-configuration]]
            [drafter.draftset :refer [->draftset-uri]]
            [drafter.main :as main]
            [drafter.rdf.sparql :as sparql]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.write-scheduler
             :refer
             [global-writes-lock queue-job! start-writer! stop-writer!]]
            [grafter-2.rdf4j.templater :refer [triplify]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf4j.repository.registry :as reg]
            [grafter.url :as url]
            [integrant.core :as ig]
            [ring.middleware.params :refer [wrap-params]]
            [ring.server.standalone :as ring-server]
            [schema.core :as s]
            [schema.test :refer [validate-schemas]]
            [swirrl-server.async.jobs :refer [create-job]]
            [clojure.pprint :as pp]
            [clojure.spec.test.alpha :as st])
  (:import grafter_2.rdf.SPARQLRepository
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
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter
           org.eclipse.rdf4j.rio.trig.TriGParserFactory))

(defn with-spec-instrumentation [f]
  (try
    (st/instrument)
    (f)
    (finally
      (st/unstrument))))

;;(use-fixtures :each validate-schemas) ;; TODO should remove this...

(defmacro TODO [& forms]
  (let [pprint-forms# (for [form forms]
                        `(pp/pprint '~form))]
    ;;`(println "TODO: fix this test form: in " ~*file* (prn-str '~@forms))
    `(do
       (println "TODO Fix" ~*file* "test:")
       ~@pprint-forms#)))

(def ^:dynamic *test-backend* nil)
(def ^:dynamic *test-writer* nil)
(def ^:dynamic *test-system* nil)

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
        encoded-auth (util/str->base64 unencoded-auth)]
    (-> request
        (assoc :identity user)
        (assoc-in [:headers "Authorization"] (str "Basic " encoded-auth)))))

(defn wait-for-lock-ms [lock period-ms]
  (if (.tryLock lock period-ms (TimeUnit/MILLISECONDS))
    (.unlock lock)
    (throw (RuntimeException. (str "Lock not released after " period-ms "ms")))))

(defmacro with-system
  "Convenience macro to build a drafter system and shut it down
  outside of with-system's lexical scope."
  ([binding-form form]
   `(with-system nil ~binding-form ~form))

  ([start-keys [binding-form system-cfg] form]
   `(let [system# (main/start-system! (main/read-system (io/resource ~system-cfg)) ~start-keys)
          ~binding-form system#
          ;; drafter specific gunk that we can ultimately remove
          configured-factories# (reg/registered-parser-factories)]
      ;; Some tests need to load and parse trig file data drafter specific gunk..  can remove when we generalise
      (reg/register-parser-factory! :construct TriGParserFactory)
      (try
        ~form
        (finally
          (ig/halt! system#)

          ;; drafter specific gunk... can remove when we generalise
          (reg/register-parser-factories! configured-factories#))))))

(defmacro deftest-system
  ([name binding-form & forms]
   `(deftest ~name
      (with-system ~binding-form
        (do ~@forms)))))

(defmacro deftest-system-with-keys
  [name start-keys binding-form & forms]
  `(deftest ~name
     (with-system ~start-keys ~binding-form
       (do ~@forms))))

(defn ^{:deprecated "Use with-system instead."} wrap-system-setup
  "Start an integrant test system.  Uses dynamic bindings to support
  old test suite style.  For new code please try the with-system maro
  instead."
  [system start-keys]
  (fn [test-fn]
    (let [started-system (main/start-system! (main/read-system (io/resource system)) start-keys)
          backend (:drafter.stasher/repo started-system)
          writer (:drafter/write-scheduler started-system)
          configured-factories (reg/registered-parser-factories)]
      ;(assert backend (str "No backend in " system))
      (binding [*test-system* started-system
                *test-backend* backend
                *test-writer* writer]
        (do
          ;; Some tests need to load and parse trig file data
          (reg/register-parser-factory! :construct TriGParserFactory)
          (try
            (test-fn)
            (finally
              (when *test-backend* (sparql/update! *test-backend* "DROP ALL ;"))
              (when (:drafter.stasher/repo *test-system*) (sparql/update! (:drafter.stasher/repo *test-system*) "DROP ALL ;"))
              (ig/halt! started-system)
              ;; TODO change to halt system
              ;;(stop-backend backend)
              ;;(stop-writer! *test-writer*)
              (reg/register-parser-factories! configured-factories))))))))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  ([db graph triples] (import-data-to-draft! db graph triples nil util/get-current-time))
  ([db graph triples draftset-ref clock-fn]

   (create-managed-graph! db graph)
   (let [draftset-uri (and draftset-ref (url/->java-uri draftset-ref))
         draft-graph (create-draft-graph! db graph draftset-uri clock-fn)]
     (sparql/add db draft-graph triples)
     draft-graph)))

(defn make-graph-live!
  ([db live-graph-uri clock-fn]
     (make-graph-live! db live-graph-uri (test-triples (URI. "http://test.com/subject-1")) clock-fn))

  ([db live-graph-uri data clock-fn]
     (let [draft-graph-uri (import-data-to-draft! db live-graph-uri data nil clock-fn)]
       (migrate-graphs-to-live! db [draft-graph-uri] clock-fn))
     live-graph-uri))

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
  (sparql/eager-query *test-backend* (str "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
                                          "ASK WHERE {"
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
     (is (= :ok (:type job-result#))
         (str "job failed: " (pr-str job-result#)))
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
  "Returns a SPARQLRepository with a query URI matching latched-http-handler
   listening on the given port."
  [port]
  (let [uri (URI. "http" nil "localhost" port nil nil nil)
        repo (SPARQLRepository. (str uri))]
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