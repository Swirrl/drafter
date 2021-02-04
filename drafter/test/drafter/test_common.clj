(ns drafter.test-common
  (:require [clj-time.coerce :refer [to-date]]
            [clj-time.core :as clj-time]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.spec.test.alpha :as st]
            [clojure.spec.alpha :as s]
            [ring.core.spec]
            [clojure.test :refer :all]
            [drafter.backend.draftset.draft-management :refer [migrate-graphs-to-live!]]
            [drafter.main :as main]
            [drafter.rdf.sparql :as sparql]
            [drafter.util :as util]
            [drafter.write-scheduler :refer [queue-job!]]
            [environ.core :refer [env]]
            [grafter-2.rdf4j.repository.registry :as reg]
            [grafter-2.rdf4j.templater :refer [triplify]]
            [integrant.core :as ig]
            [kaocha.plugin.auth-env-plugin :refer [*auth-env*]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.server.standalone :as ring-server]
            [drafter.async.jobs :as async :refer [create-job]]
            [drafter.async.spec :as async-spec]
            [aero.core :as aero]
            [drafter.user :as user]
            [drafter.rdf.drafter-ontology :refer [drafter:endpoints]]
            [grafter.vocabularies.dcterms :refer [dcterms:modified]]
            [drafter.spec :refer [load-spec-namespaces!]]
            [drafter.write-scheduler :as scheduler]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.time :as time])
  (:import [com.auth0.jwk Jwk JwkProvider]
           com.auth0.jwt.algorithms.Algorithm
           com.auth0.jwt.JWT
           grafter_2.rdf.SPARQLRepository
           [java.io ByteArrayInputStream ByteArrayOutputStream OutputStream PrintWriter File]
           java.lang.AutoCloseable
           [java.net InetSocketAddress ServerSocket SocketException URI]
           java.nio.charset.Charset
           java.security.KeyPairGenerator
           [java.util ArrayList Scanner UUID]
           [java.util.concurrent CountDownLatch TimeUnit]
           [org.apache.http.entity ContentLengthStrategy ContentType StringEntity]
           org.apache.http.impl.entity.StrictContentLengthStrategy
           [org.apache.http.impl.io ChunkedOutputStream ContentLengthOutputStream DefaultHttpRequestParser
                                    DefaultHttpResponseWriter HttpTransportMetricsImpl IdentityOutputStream
                                    SessionInputBufferImpl SessionOutputBufferImpl]
           org.apache.http.message.BasicHttpResponse
           org.apache.http.ProtocolVersion
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter
           org.eclipse.rdf4j.rio.trig.TriGParserFactory
           [java.util.zip GZIPOutputStream]
           [java.time.temporal Temporal]
           [java.time OffsetDateTime]))

(defn with-spec-instrumentation [f]
  (try
    (load-spec-namespaces!)
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

(defn test-triples
  ([] (test-triples (URI. "http://test.com/subject-1")))
  ([subject-uri]
   (triplify [subject-uri
              [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/1")]
              [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/2")]])))

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

(defonce keypair
  (-> (KeyPairGenerator/getInstance "RSA")
      (doto (.initialize 4096))
      (.genKeyPair)))

(def pubkey (.getPublic keypair))
(def privkey (.getPrivate keypair))

(def alg (Algorithm/RSA256 pubkey privkey))

(defn token [iss aud sub role]
  (-> (JWT/create)
      (.withIssuer (str iss \/))
      (.withSubject sub)
      (.withAudience (into-array String [aud]))
      (.withExpiresAt (to-date (clj-time/plus (clj-time/now) (clj-time/minutes 10))))
      (.withClaim "scope" role)
      (.sign alg)))

(defn mock-jwk []
  (reify JwkProvider
    (get [_ _]
      (proxy [Jwk] ["" "" "RSA" "" '() "" '() "" {}]
        (getPublicKey [] (.getPublic keypair))))))

(defmethod ig/init-key :drafter.auth.auth0/mock-jwk [_ {:keys [endpoint] :as opts}]
  (mock-jwk))

(defn user-access-token [user-id scope]
  (token (env :auth0-domain) (env :auth0-aud) user-id scope))

(defn set-auth-header [request access-token]
  (assoc-in request [:headers "Authorization"] (str "Bearer " access-token)))

(defn with-identity
  "Sets the given test user as the user on a request"
  [{:keys [email role] :as user} request]
  ;; TODO: this is a bit gross but, we need to switch implementation of this
  ;; mocky thing based on the type of auth provider we're currently testing.
  (case *auth-env*
    :auth0
    (-> request
        (assoc :identity user)
        (set-auth-header (user-access-token email (str "drafter" role))))
    (let [unencoded-auth (str (user/username user) ":" "password")
          encoded-auth (util/str->base64 unencoded-auth)]
      (-> request
          (assoc :identity user)
          (assoc-in [:headers "Authorization"] (str "Basic " encoded-auth))))))


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
   `(let [config# (aero/read-config (io/resource ~system-cfg) {:profile *auth-env*})
          system# (main/start-system! config# ~start-keys)
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
            (when *test-backend* (sparql/update! *test-backend* "DROP ALL ;"))
            (when (:drafter.stasher/repo *test-system*) (sparql/update! (:drafter.stasher/repo *test-system*) "DROP ALL ;"))

            (test-fn)
            (finally
              (ig/halt! started-system)
              ;; TODO change to halt system
              ;;(stop-backend backend)
              ;;(stop-writer! *test-writer*)
              (reg/register-parser-factories! configured-factories))))))))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  [db graph triples draftset-ref]
  (let [graph-manager (graphs/create-manager db)
        draft-graph (graphs/create-user-graph-draft graph-manager draftset-ref graph)]
    (sparql/add db draft-graph triples)
    draft-graph))

(defn make-graph-live!
  ([db live-graph-uri] (make-graph-live! db live-graph-uri (test-triples (URI. "http://test.com/subject-1"))))

  ([db live-graph-uri data]
     (make-graph-live! db live-graph-uri data time/system-clock))

  ([db live-graph-uri data clock]
     (let [test-publisher (user/create-user "publisher@swirrl.com" :publisher (user/get-digest "password"))
           draftset-id (dsops/create-draftset! db test-publisher)
           draft-graph-uri (import-data-to-draft! db live-graph-uri data draftset-id)]
       (migrate-graphs-to-live! db [draft-graph-uri] clock))
     live-graph-uri))

(defn during-exclusive-write-f [global-writes-lock f]
  (let [p (promise)
        latch (CountDownLatch. 1)
        exclusive-job (create-job nil  {} :publish-write (fn [j]
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

(defmacro during-exclusive-write [global-writes-lock & forms]
  `(during-exclusive-write-f ~global-writes-lock (fn [] ~@forms)))

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

(def default-timeout 20000)

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
  ([path] (await-completion path default-timeout))
  ([path timeout]
   (let [start (System/currentTimeMillis)]
     (loop [guid (job-path->job-id path)]
       (if-let [job (async/complete-job guid)]
         @(:value-p job)
         (if (> (System/currentTimeMillis) (+ start (or timeout default-timeout) ))
           (throw (RuntimeException. "Timed out awaiting test value"))
           (do
             (Thread/sleep 5)
             (recur guid))))))))

(defn await-success
  "Waits for the job with the given path to be present in the given
  job state atom and then asserts the job succeeded. Returns the job
  result map."
  [job-path]
  (let [job-result (await-completion job-path)]
    (is (= :ok (:type job-result))
        (str "job failed: " (pr-str job-result)))
    job-result))

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

(defn assert-spec [spec x]
  (is (nil? (s/explain-data spec x))))

(defn deny-spec [spec x]
  (is (some? (s/explain-data spec x))))

(defn- status-matches? [status-code]
  (fn [response] (= status-code (:status response))))

(defn response-code-spec [code]
  (s/and :ring/response (status-matches? code)))

(defn assert-is-ok-response [response]
  (assert-spec (response-code-spec 200) response))

(defn assert-is-accepted-response [response]
  (assert-spec (response-code-spec 202) response))

(defn assert-is-no-content-response [response]
  (assert-spec (response-code-spec 204) response))

(defn assert-is-not-found-response [response]
  (assert-spec (response-code-spec 404) response))

(defn assert-is-not-acceptable-response [response]
  (assert-spec (response-code-spec 406) response))

(defn assert-is-unprocessable-response [response]
  (assert-spec (response-code-spec 422) response))

(defn assert-is-payload-too-large-response [response]
  (assert-spec (response-code-spec 413) response))

(defn assert-is-unsupported-media-type-response [response]
  (assert-spec (response-code-spec 415) response))

(defn assert-is-method-not-allowed-response [response]
  (assert-spec (response-code-spec 405) response))

(defn assert-is-forbidden-response [response]
  (assert-spec (response-code-spec 403) response))

(defn assert-is-unauthorised-response [response]
  (assert-spec (response-code-spec 401) response))

(defn assert-is-bad-request-response [response]
  (assert-spec (response-code-spec 400) response))

(defn assert-is-server-error [response]
  (assert-spec (response-code-spec 500) response))

(defn assert-is-service-unavailable-response [response]
  (assert-spec (response-code-spec 503) response))

(defn exec-and-await-job
  "Executes a job and waits the specified timeout period for it to complete.
   Returns the result of the job if it completed within the timeout period."
  ([job] (exec-and-await-job job 10))
  ([{:keys [value-p] :as job} timeout-seconds]
   (scheduler/queue-job! job)
   (let [result (deref value-p (* 1000 timeout-seconds) ::timeout)]
     (when (= ::timeout result)
       (throw (ex-info (format "Job failed to complete after %d seconds" timeout-seconds)
                       {:job job})))
     result)))

(defn exec-and-await-job-success
  "Executes a job, waits for it to finish and asserts the result was successful.
   Waits the specified timeout period or 10 seconds if none is specified."
  ([job] (exec-and-await-job-success job 10))
  ([job timeout-seconds]
   (let [result (exec-and-await-job job timeout-seconds)]
     (assert-spec ::async-spec/success-job-result result))))

(defn string->input-stream [s]
  (ByteArrayInputStream. (.getBytes s)))

(defmacro with-temp-file
  "Creates a temp file with the given file name prefix and suffix and executes
   body wit the resulting File object bound to the name specified by binding"
  [prefix suffix binding & body]
  `(let [~binding (File/createTempFile ~prefix ~suffix)]
     (try
       ~@body
       (finally (.delete ~binding)))))

(defn ->gzip-input-stream
  "Writes source gzip-compressed to an in-memory buffer and returns an
   input stream"
  [source]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [gzos (GZIPOutputStream. baos)]
      (io/copy source gzos))
    (ByteArrayInputStream. (.toByteArray baos))))

(defn equal-up-to
  "Returns whether two Temporal instances are within the specified interval of each other"
  [^Temporal t1 ^Temporal t2 limit units]
  (< (.until t1 t2 units) limit))

(defn get-public-endpoint-triples [repo]
  (sparql/eager-query repo (select-all-in-graph drafter:endpoints)))

(defn is-modified-statement? [s]
  (= dcterms:modified (:p s)))

(defn remove-updated [endpoint-triples]
  (remove is-modified-statement? endpoint-triples))

(defmacro check-endpoint-graph-consistent
  "Macro to check the public endpoints graph is not corrupted by the actions executed
   in forms. The statements in the endpoint graphs must be equal before and after executing
   forms with the possible exception that the updated time of the public endpoint can be
   updated"
  [system & forms]
  `(let [repo# (:drafter/backend ~system)
        triples-before# (get-public-endpoint-triples repo#)]
    ~@forms
    (let [triples-after# (get-public-endpoint-triples repo#)]
      (is (= (set (map :p triples-before#)) (set (map :p triples-after#))))
      (is (= (set (remove-updated triples-before#)) (set (remove-updated triples-after#)))))))

(defn incrementing-clock
  "Returns a clock which returns a later time every time it is queried"
  ([] (incrementing-clock (time/system-now)))
  ([start-time]
   (let [current (atom start-time)]
     (reify time/Clock
       (now [_] (swap! current (fn [^OffsetDateTime current] (.plusSeconds current 1))))))))

(defrecord ManualClock [current]
  time/Clock
  (now [_this] @current))

(defn manual-clock
  "Creates a clock that continually returns the given time until updated with the set-now
   and advance-by functions"
  [initial-time]
  (->ManualClock (atom initial-time)))

(defn set-now
  "Sets the time on the manual clock to the given instant"
  [{:keys [current] :as manual-clock} now]
  (reset! current now)
  nil)

(defn advance-by
  "Advances the current time on the manual clock by the given period"
  [{:keys [current] :as manual-clock} period]
  (swap! current (fn [^OffsetDateTime t] (.plus t period)))
  nil)