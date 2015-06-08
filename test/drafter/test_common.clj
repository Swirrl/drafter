(ns drafter.test-common
  (:require [clojure.test :refer :all]
            [grafter.rdf.repository :refer :all]
            [grafter.rdf.templater :refer [triplify]]
            [me.raynes.fs :as fs]
            [drafter.rdf.draft-management :refer [lookup-draft-graph-uri import-data-to-draft! migrate-live!]]
            [drafter.write-scheduler :refer [start-writer! stop-writer!]])
  (:import [java.util Scanner]))

(def ^:dynamic *test-db* (repo (memory-store)))

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

(declare ^:dynamic *test-writer*)

(defn wrap-with-clean-test-db
  "Sets up a native store and starts a drafter-writer thread to ensure
  operations are written.  The writer should be stopped and GC'd when
  the scope is closed."
  ([test-fn] (wrap-with-clean-test-db identity test-fn))
  ([setup-state-fn test-fn]
   (binding [*test-db* (repo (native-store test-db-path))
             *test-writer* (start-writer!)]
     (try
       (setup-state-fn *test-db*)
       (test-fn)
       (finally
         (fs/delete-dir test-db-path)
         (stop-writer! *test-writer*))))))

(defn make-store []
  (repo))

(defn make-graph-live!
  ([db live-guri]
     (make-graph-live! db live-guri (test-triples "http://test.com/subject-1")))

  ([db live-guri data]
     (let [draft-guri (import-data-to-draft! db live-guri data)]
       (migrate-live! db draft-guri))
     live-guri))
