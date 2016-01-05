(ns drafter.test-common
  (:require [clojure.test :refer :all]
            [grafter.rdf.repository :refer :all]
            [grafter.rdf.protocols :refer [add update!]]
            [grafter.rdf.templater :refer [triplify]]
            [environ.core :refer [env]]
            [drafter.backend.sesame.common.protocols :refer [->sesame-repo]]
            [drafter.backend.configuration :refer [get-backend]]
            ;; [drafter.backend.sesame.native]
            ;; [drafter.backend.sesame.remote]
            [drafter.backend.protocols :refer [stop]]
            [me.raynes.fs :as fs]
            [drafter.rdf.draft-management :refer [create-managed-graph! create-draft-graph!
                                                  migrate-live!]]
            [drafter.write-scheduler :refer [start-writer! stop-writer! queue-job!
                                             global-writes-lock]]
            [swirrl-server.async.jobs :refer [create-job]])
  (:import [java.util Scanner]
           [java.util.concurrent CountDownLatch TimeUnit]))

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

(defn wait-for-lock-ms [lock period-ms]
  (if (.tryLock lock period-ms (TimeUnit/MILLISECONDS))
    (.unlock lock)
    (throw (RuntimeException. (str "Lock not released after " period-ms "ms")))))

(declare ^:dynamic *test-writer*)

(defn wrap-db-setup [test-fn]
  (let [backend (get-backend (assoc env :drafter-repo-path "test-drafter-db"))]
    (println "backend: " backend)
    (binding [*test-backend* backend
              *test-writer* (start-writer!)]

      (try
          (test-fn)
          (finally
            (stop backend)
            (stop-writer! *test-writer*))))))

(defn wrap-clean-test-db
  ([test-fn] (wrap-clean-test-db identity test-fn))
  ([setup-state-fn test-fn]
   (update! *test-backend*
            "DROP ALL ;")
   (setup-state-fn *test-backend*)
   (test-fn)))


(defn make-store []
  (repo))

(defn make-backend []
  (get-backend env))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  [db graph triples]

  (create-managed-graph! db graph)
  (let [draft-graph (create-draft-graph! db graph)]
    (add db draft-graph triples)
    draft-graph))

(defn make-graph-live!
  ([db live-guri]
     (make-graph-live! db live-guri (test-triples "http://test.com/subject-1")))

  ([db live-guri data]
     (let [draft-guri (import-data-to-draft! db live-guri data)]
       (migrate-live! db draft-guri))
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

(defn ask? [& graphpatterns]
  "Bodgy convenience function for ask queries"
  (query *test-backend* (str "ASK WHERE {"

                        (-> (apply str (interpose " " graphpatterns))
                            (.replace " >" ">")
                            (.replace "< " "<"))

                        "}")))
