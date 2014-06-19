(ns drafter.rdf.sesame
  (:require [grafter.rdf.ontologies.rdf :refer :all]
            [grafter.rdf.sesame :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [clojure.java.io :as io]
            [grafter.rdf :refer [s graph graphify load-triples add-properties]]
            [grafter.rdf.protocols :refer [add subject predicate object context
                                           add-statement statements begin commit rollback]]
            [pandect.core :as digest])
  (:import [java.util Date]))

(def drafter-state-graph "http://publishmydata.com/graphs/drafter/drafts")

(def staging-base "http://publishmydata.com/graphs/drafter/draft")

(def to-quads (partial graph drafter-state-graph))

(defn make-draft-graph-uri []
  (str staging-base "/" (java.util.UUID/randomUUID)))

(defn create-managed-graph
  "Returns some RDF statements to represent the ManagedGraphs state."
  ([graph-uri] (create-managed-graph graph-uri {}))
  ([graph-uri meta-data]
     (let [rdf-template [graph-uri
                         [rdf:a drafter:ManagedGraph]
                         [drafter:isPublic false]]]

       (add-properties rdf-template meta-data))))

(defn is-graph-managed? [db graph-uri]
  (query db
   (str "ASK WHERE {
        <" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ."
        "
         }")))

(defn create-managed-graph!
  ([db graph-uri] (create-managed-graph! db graph-uri {}))
  ([db graph-uri opts]
     (let [managed-graph-quads (to-quads (create-managed-graph graph-uri opts))]
       (when (is-graph-managed? db graph-uri)
         (throw (ex-info "This graph already exists" {:type :graph-exists})))
       (add db managed-graph-quads))))

(defn create-draft-graph
  ([live-graph-uri draft-graph-uri time]
     (create-draft-graph live-graph-uri draft-graph-uri time {}))
  ([live-graph-uri draft-graph-uri time opts]

     [[live-graph-uri
       [drafter:hasDraft draft-graph-uri]]

      [draft-graph-uri
       [rdf:a drafter:DraftGraph]
       [drafter:modifiedAt time]]]))

(defn create-draft-graph!
  "Creates a new draft graph with a unique graph name, expects the
  live graph to already be created."
  ([db live-graph-uri]
     (create-draft-graph! db live-graph-uri {}))
  ([db live-graph-uri opts]
     (let [now (Date.)
           draft-graph-uri (make-draft-graph-uri)]

       (add db (->> (create-draft-graph live-graph-uri draft-graph-uri now)
                    (apply to-quads)))
       draft-graph-uri)))

(defn append-data!
  [db draft-graph-uri triples]

  (add db draft-graph-uri triples))

(defn replace-data!
  [db draft-graph-uri triples]
  (update! db (str "DROP GRAPH <" draft-graph-uri ">"))
  (add db draft-graph-uri triples))

(comment
  (defn has-management-graph? [db graph]
    (let [staging-graph (->staging-graph graph)]
      (query db (str "ASK WHERE {
                      GRAPH <" drafter-state-graph "> {
                         <" staging-graph "> ?p ?o .
                      }
                    }"))))

  (defn select-graphs [graph-type db]
    (query db (str "SELECT ?graph WHERE {
                     GRAPH <" drafter-state-graph "> {"
                     "?graph " drafter:hasGraph " " graph-type " . "
                     "}"
                     "}")))

  (defn is-live? [db graph]
    (let [staging-graph (->staging-graph graph)]
      (query db (str "ASK WHERE {
                      GRAPH <" drafter-state-graph "> {
                         <" staging-graph "> <> ?o .
                      }
                    }"))))

  (def live-graphs (partial select-graphs drafter:Live))
  (def draft-graphs (partial select-graphs drafter:Draft))

  (defn make-draft
    ([db graf]
       (make-draft db graf {}))

    ([db graf metadata]
       (with-transaction db
         (let [graph-state (managed-graph graf metadata)
               state-graph-uri (first graph-state)]

           (add db (graph drafter-state-graph
                          graph-state))
           state-graph-uri)))))



(defn import-data-to-draft [db graph triples]

  ;; 1. generate a unique graph id for staging graph.
  ;; 2. Create graph in state staging add triples to it leave staging
  ;; graph in place so we don't need to take a copy of it to build
  ;; next version of staging graph.

  (comment (let [staging-graph (->staging-graph graph)]
             (with-transaction db
               (add db (managed-graph graph))
               (add db triples)))))

(defn migrate-graph [db graph]
  ;; 1. remove the destination graph
  ;; 2. lookup staging graph
  ;; 3. copy staging graph to "live graph name"
  ;; 4. leave staging graph in place for future staging changes
  )

(defn delete-graph [db graph]
  ;; 1. lookup staging graph
  ;; 2. remove staging graph
  )

(defn rename-graph [db old-graph new-graph]
  ;; lookup old-graph
  ;; calculate new graph sha
  ;; copy data/state to new graph name
  ;; remove old graph name
  ;; update subject name to new-graph uris in metadata graph.
  )

(comment
  (add (rdf-serializer "test.ttl")
       (graph "http://foo.com/" ["http://foo.com/" ["http://foo.com/" "http://foo.com/"]]))
  )
