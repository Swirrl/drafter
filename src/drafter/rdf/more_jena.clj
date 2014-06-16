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



(comment

  (def db (TDBFactory/createDataset store-directory))

  (def triples (with-transaction :write db
                 (let [triples (make-model-from-triples db "http://data.opendatascotland.org/resource.ttl?uri=http%3A%2F%2Fdata.opendatascotland.org%2Fdata%2Fgeography%2Fcouncil-areas")]
                   (println triples)
                   triples)))

  (with-transaction :write db (.addNamedModel db "http://fooobar.com/" triples))

  (load-triples-into-graph db "http://example.org/graph/eldis" "eldis.nt")

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
