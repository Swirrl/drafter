(ns drafter.test-common
  (:require [clojure.test :refer :all]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :refer [add]]
            [grafter.rdf.templater :refer [triplify]]
            [grafter.rdf.repository.registry :as reg]
            [environ.core :refer [env]]
            [drafter.user :as user]
            [drafter.backend.configuration :refer [get-backend]]
            [drafter.backend.protocols :refer [stop-backend]]
            [drafter.rdf.draft-management :refer [create-managed-graph! migrate-graphs-to-live!
                                                  create-draft-graph! query update!]]
            [drafter.draftset :refer [->draftset-uri]]
            [drafter.write-scheduler :refer [start-writer! stop-writer! queue-job!
                                             global-writes-lock]]
            [swirrl-server.async.jobs :refer [create-job]]
            [schema.test :refer [validate-schemas]]
            [schema.core :as s])
  (:import [java.util Scanner UUID]
           [java.util.concurrent CountDownLatch TimeUnit]
           [java.io ByteArrayInputStream]
           org.openrdf.rio.trig.TriGParserFactory))


(use-fixtures :each validate-schemas)

(def ^:dynamic *test-backend*)

(def test-db-path "drafter-test-db")

(defn test-triples [subject-uri]
  (triplify [subject-uri
             ["http://test.com/hasProperty" "http://test.com/data/1"]
             ["http://test.com/hasProperty" "http://test.com/data/2"]]))

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
  (let [backend (get-backend (assoc env :drafter-repo-path "test-drafter-db"))
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
   (update! *test-backend*
            "DROP ALL ;")
   (setup-state-fn *test-backend*)
   (test-fn)))

(defn make-store []
  (repo/repo))

(defn make-backend []
  (get-backend env))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  ([db graph triples] (import-data-to-draft! db graph triples nil))
  ([db graph triples draftset-ref]

   (create-managed-graph! db graph)
   (let [draftset-uri (and draftset-ref (str (->draftset-uri draftset-ref)))
         draft-graph (create-draft-graph! db graph {} draftset-uri)]
     (add db draft-graph triples)
     draft-graph)))

(defn make-graph-live!
  ([db live-guri]
     (make-graph-live! db live-guri (test-triples "http://test.com/subject-1")))

  ([db live-guri data]
     (let [draft-guri (import-data-to-draft! db live-guri data)]
       (migrate-graphs-to-live! db [draft-guri]))
     live-guri))

(defn during-exclusive-write-f [f]
  (let [p (promise)
        latch (CountDownLatch. 1)
        exclusive-job (create-job :exclusive-write
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
  (query *test-backend* (str "ASK WHERE {"

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

(defn string->input-stream [s]
  (ByteArrayInputStream. (.getBytes s)))
