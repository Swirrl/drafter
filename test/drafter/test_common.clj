(ns drafter.test-common
  (:require [clojure.test :refer :all]
            [grafter.rdf.sesame :refer :all]
            [grafter.rdf :refer [triplify]]
            [me.raynes.fs :as fs])
  (:import [java.util Scanner]))

(def ^:dynamic *test-db* (repo (memory-store)))

(def test-db-path "MyDatabases/repositories/test-db")

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

(defn wrap-with-clean-test-db
  ([test-fn] (wrap-with-clean-test-db identity test-fn))
  ([setup-state-fn test-fn]
     (try
       (binding [*test-db* (repo (native-store test-db-path))]
         (setup-state-fn *test-db*)
         (test-fn))
       (finally
         (fs/delete-dir test-db-path)))))
