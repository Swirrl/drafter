(ns drafter.rdf.draft-management
  (:require [grafter.rdf.ontologies.rdf :refer :all]
            [grafter.rdf.sesame :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [taoensso.timbre :as timbre]
            [clojure.java.io :as io]
            [grafter.rdf :refer [graph graphify load-triples add-properties]]
            [grafter.rdf.protocols :refer [add subject predicate object context
                                           add-statement statements begin commit rollback]])
  (:import [java.util Date]))

(def drafter-state-graph "http://publishmydata.com/graphs/drafter/drafts")

(def staging-base "http://publishmydata.com/graphs/drafter/draft")

(def to-quads (partial graph drafter-state-graph))

(defn make-draft-graph-uri []
  (str staging-base "/" (java.util.UUID/randomUUID)))

(defn is-graph-managed? [db graph-uri]
  (query db
   (str "ASK WHERE {
        <" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ."
        "}")))

(defn create-managed-graph
  "Returns some RDF statements to represent the ManagedGraphs state."
  ([graph-uri] (create-managed-graph graph-uri {}))
  ([graph-uri meta-data]
     (let [rdf-template [graph-uri
                         [rdf:a drafter:ManagedGraph]
                         [drafter:isPublic false]]]
       ; add the options as extra properties
       (add-properties rdf-template meta-data))))

(defn create-managed-graph!
  ([db graph-uri] (create-managed-graph! db graph-uri {}))
  ([db graph-uri opts]
     ; We only do anything if it's not already a managed graph

     ; FIXME: Is this a potential race condition? i.e. we check for existence (and it's false) and before executing someone else makes
     ;   the managed graph(?). Ideally, we'd do this as a single INSERT/WHERE statement.
     (if (not (is-graph-managed? db graph-uri))
       (let [managed-graph-quads (to-quads (create-managed-graph graph-uri opts))]
       (add db managed-graph-quads)))))

(defn create-draft-graph
  ([live-graph-uri draft-graph-uri time]
     (create-draft-graph live-graph-uri draft-graph-uri time {}))
  ([live-graph-uri draft-graph-uri time opts]

     (let [
             live-graph-triples [live-graph-uri
                               [drafter:hasDraft draft-graph-uri]]
             draft-graph-triples  [draft-graph-uri
                                    [rdf:a drafter:DraftGraph]
                                    [drafter:modifiedAt time]]
             triples [live-graph-triples (add-properties draft-graph-triples
                                                         ; we need to make the values of the opts into strings by calling `s`.
                                                         (into {} (for [[k v] opts] [k ((fn[v] (s v) ) v)])))]]
         triples))) ; returns the triples

(defn create-draft-graph!
  "Creates a new draft graph with a unique graph name, expects the
  live graph to already be created."
  ([db live-graph-uri]
     (create-draft-graph! db live-graph-uri {}))
  ([db live-graph-uri opts]
     (let [now (Date.)
           draft-graph-uri (make-draft-graph-uri)]
       ; adds the triples returned by crate-draft-graph to the state graph
       (add db (->> (create-draft-graph live-graph-uri draft-graph-uri now opts)
                    (apply to-quads)))

       draft-graph-uri))) ; returns the draft-graph-uri

(defn append-data!
  [db draft-graph-uri triples]
  (add db draft-graph-uri triples))

(defn set-isPublic! [db live-graph-uri boolean-value]
  (let [query-str (str "DELETE {"
                  "GRAPH <" drafter-state-graph "> {"
                  "<" live-graph-uri "> <" drafter:isPublic  "> " (not boolean-value) " ."
                  "}"
                "} INSERT {"
                  "GRAPH <" drafter-state-graph "> {"
                    "<" live-graph-uri "> <" drafter:isPublic  "> " boolean-value " ."
                    "}"
                  "} WHERE {"
                    "GRAPH <" drafter-state-graph "> {"
                      "<" live-graph-uri "> <" drafter:isPublic  "> " (not boolean-value) " ."
                    "}"
                  "}")]

    (update! db
             query-str)))

(defn delete-graph! [db graph-uri]
  (timbre/info (str "Deleting graph... " graph-uri))
  (update! db (str "DROP GRAPH <" graph-uri ">"))
  (timbre/info (str "Deleted graph " graph-uri))

  (timbre/info (str "Deleting draft graph from state ... " graph-uri))
  ; if the graph-uri is a draft graph uri,
  ;   remove the mention of this draft uri, but leave the live graph as a managed graph.
  (let [query-str (str "DELETE {"
                  "GRAPH <" drafter-state-graph "> {"
                     "?live <" drafter:hasDraft "> <" graph-uri "> . "
                     "<" graph-uri "> ?p ?o . "
                  "}"
                "} WHERE {"
                    "GRAPH <" drafter-state-graph "> {"
                       "?live <" rdf:a "> <" drafter:ManagedGraph "> ; "
                             "<" drafter:hasDraft "> <" graph-uri "> . "
                       "<" graph-uri "> ?p ?o . "
                    "}"
                  "}")]
    (update! db
             query-str))
  (timbre/info (str "Deleted draft graph from state " graph-uri))

  ;(timbre/info (str "updating live state for " graph-uri))
  ;(set-isPublic! db graph-uri false) ; just make it not public. Is this the right thing?
  ;(timbre/info (str "updated live state for" graph-uri))
)

(defn replace-data!
  [db draft-graph-uri triples]
  (delete-graph! db draft-graph-uri)
  (add db draft-graph-uri triples))

(defn lookup-live-graph [db draft-graph-uri]
  "Given a draft graph URI, lookup and return its live graph."
  (let [live (-> (query db
                        (str "SELECT ?live WHERE {"
                               "?live <" rdf:a "> <" drafter:ManagedGraph "> ; "
                                     "<" drafter:hasDraft "> <" draft-graph-uri "> . "
                             "} LIMIT 1"))
                 first
                 (get "live")
                 str)]
    live))

(defn draft-graphs [db]
  "Get all draft graphs"
  (->> (query db
              (str "SELECT ?draft WHERE {"
                     "?live <" drafter:hasDraft "> ?draft ."
                     "}"))
       (map #(str (% "draft")))
       (into #{})))

(defn live-graphs [db & {:keys [online] :or {online true}}]
  "Get all live graph names.  Takes an optional boolean keyword
  argument of :online to allow querying for all online/offline live
  graphs."
  (->> (query db
              (str "SELECT ?live WHERE {"
                   "?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                         "<" drafter:isPublic  "> " online " ."
                   "}"))
       (map #(str (% "live")))
       (into #{})))



(defn migrate-live!
  "Moves the triples from the draft graph to the draft graphs live destination."
  [db draft-graph-uri]

  (if-let [live-graph-uri (lookup-live-graph db draft-graph-uri)]
    (do
      (timbre/info (str "Migrating graph: " draft-graph-uri " to live graph: " live-graph-uri))
      (delete-graph! db live-graph-uri)
      (add db live-graph-uri
           (query db
                  (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }")))
      (delete-graph! db draft-graph-uri)
      (set-isPublic! db live-graph-uri true)
      (timbre/info (str "Migrated graph: " draft-graph-uri " to live graph: " live-graph-uri)))

    (throw (ex-info (str "Could not find the live graph associated with graph " draft-graph-uri)))))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  [db graph triples]

  (create-managed-graph! db graph)
  (let [draft-graph (create-draft-graph! db graph)]
    (add db draft-graph triples)
    draft-graph))

(comment (let [staging-graph (->staging-graph graph)]
           (with-transaction db
             (add db (managed-graph graph))
             (add db triples))))


(defn rename-graph [db old-graph new-graph]
  ;; lookup old-graph
  ;; calculate new graph sha
  ;; copy data/state to new graph name
  ;; remove old graph name
  ;; update subject name to new-graph uris in metadata graph.
  )
