(ns drafter.rdf.more-jena
  (:require [drafter.rdf.jena :refer :all])
  (:import [com.hp.hpl.jena.tdb TDBFactory TDB]
           [com.hp.hpl.jena.query
            QueryFactory QueryExecutionFactory
            QueryExecution Syntax Query ReadWrite
            ResultSetFormatter]
           [com.hp.hpl.jena.update GraphStore GraphStoreFactory UpdateExecutionFactory
            UpdateFactory UpdateProcessor UpdateRequest]
           [com.hp.hpl.jena.rdf.model ModelFactory]
           [org.apache.jena.riot Lang RDFDataMgr]))

(def store-directory "MyDatabases/db")

(defn create-dataset-with-data [ds]
  (let [dataset (->jena-triple-store ds)]
    (with-transaction :write dataset
      (let [graph-store (GraphStoreFactory/create dataset)
            update-str "PREFIX : <http://example/>
                        INSERT { :s6 :p ?now.
                                 :s7 :p ?now. } WHERE { BIND(now() AS ?now) }"
            update-str2 "PREFIX : <http://example/>
                        INSERT { :s6 :p ?now.
                                 :s9 :p ?now. } WHERE { BIND(now() AS ?now) }"]

        (exec-update update-str graph-store)
        (exec-update update-str2 graph-store)))
    (.close dataset)))

(defn- make-model-from-triples [dataset triple-source]
  "Creates a JENA model/graph from the supplied triples.  Attempts to
  resolve the triple-source String as either a filename or URI to load
  the triples from.  Doesn't name the graph of triples or explicitly
  add them to the dataset."
  (let [model (.getDefaultModel dataset)]
    (RDFDataMgr/read model triple-source)
    model))

(defn load-triples-into-graph [dataset graph-uri triple-source]
  (with-transaction :write dataset
    (let [model (make-model-from-triples dataset triple-source)]
      (.addNamedModel dataset graph-uri model))))



(comment

  (def db (TDBFactory/createDataset store-directory))

  (with-transaction :write db
    (let [triples (make-model-from-triples db "http://data.opendatascotland.org/resource.ttl?uri=http%3A%2F%2Fdata.opendatascotland.org%2Fdata%2Fgeography%2Fcouncil-areas")]
      (println triples)
      triples))

  (load-triples-into-graph db "http://example.org/graph/eldis" "eldis.nt")
  (future (load-triples-into-graph db "http://example.org/graph/eldis2" "eldis.nt"))

  (def f1 (future (load-triples-into-graph db "http://example.org/graph/eldis10" "eldis.nt") :done1))

  (defn query-loop []
    (loop []
      (query-seq db [result
                  "SELECT (COUNT(?s) AS ?scount) ?g WHERE {
                     GRAPH ?g {
                       ?s ?p ?o .
                     }
                   } GROUP BY ?g"]

                 (println result))
      (Thread/sleep 1000)
      (recur)))

  (load-triples-into-graph db "http://example.org/graph/eldis6" "eldis.nt")

  )
