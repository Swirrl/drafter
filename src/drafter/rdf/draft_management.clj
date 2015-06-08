(ns drafter.rdf.draft-management
  (:require [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter.rdf :refer [add s]]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.repository :refer [query evaluate prepare-query]]
            [grafter.rdf.templater :refer [add-properties graph]])
  (:import (java.util Date UUID)
           (org.openrdf.model.impl URIImpl)))

(def drafter-state-graph "http://publishmydata.com/graphs/drafter/drafts")

(def staging-base "http://publishmydata.com/graphs/drafter/draft")

(def to-quads (partial graph drafter-state-graph))

(defn make-draft-graph-uri []
  (str staging-base "/" (UUID/randomUUID)))

(defn with-state-graph
  "Wraps the string in a SPARQL
   GRAPH <http://publishmydata.com/graphs/drafter/draft> {
     <<sparql-fragment>>
   } clause."

  [& sparql-string]
  (apply str " GRAPH <" drafter-state-graph "> { "
         (concat sparql-string
                 " }")))

(defn is-graph-managed? [db graph-uri]
  (query db
         (str "ASK WHERE {"
              (with-state-graph
                "<" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ."
                "}")
              )))

(defn draft-exists?
  "Checks state graph to see if a draft graph exists"
  [db graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT ?s WHERE {"
                 (with-state-graph "<" graph-uri ">  a <" drafter:DraftGraph "> .")
                 "  }"
                 "  LIMIT 1"
                 "}")]
    (query db qry)))

(defn has-more-than-one-draft?
  "Given a live graph uri, check to see if it is referenced by more
  than one draft in the state graph."
  [db live-graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT (COUNT(?draft) AS ?numberOfRefs)   WHERE {"
                 "    {"
                 "      <" live-graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ;"
                 "        <" drafter:hasDraft "> ?draft ."
                 "    }"
                 "  }"
                 "  HAVING (?numberOfRefs > 1)"
                 "}")]
    (query db qry)))

(defn graph-exists?
  "Checks that a graph exists"
  [db graph-uri]
  (query db
         (str "ASK WHERE {"
              "  SELECT ?s ?p ?o WHERE {"
              "    GRAPH <" graph-uri "> { ?s ?p ?o }"
              "  }"
              "  LIMIT 1"
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
  "Create a record of a managed graph in the database, returns the
  graph-uri that was passed in."
  ([db graph-uri] (create-managed-graph! db graph-uri {}))
  ([db graph-uri meta-data]
     ;; We only do anything if it's not already a managed graph

     ;; FIXME: Is this a potential race condition? i.e. we check for existence (and it's false) and before executing someone else makes
     ;; the managed graph(?). Ideally, we'd do this as a single INSERT/WHERE statement.
     (if (not (is-graph-managed? db graph-uri))
       (let [managed-graph-quads (to-quads (create-managed-graph graph-uri meta-data))]
         (add db managed-graph-quads)))
       graph-uri))

(defn create-draft-graph
  ([live-graph-uri draft-graph-uri time]
     (create-draft-graph live-graph-uri draft-graph-uri time {}))
  ([live-graph-uri draft-graph-uri time opts]

     (let [live-graph-triples [live-graph-uri
                               [drafter:hasDraft draft-graph-uri]]
           draft-graph-triples  [draft-graph-uri
                                 [rdf:a drafter:DraftGraph]
                                 [drafter:modifiedAt time]]
           triples [live-graph-triples (add-properties draft-graph-triples
                                                       ;; we need to make the values of the opts into strings by calling `s`.
                                                       (into {} (for [[k v] opts]
                                                                  [k (s v)])))]]
       triples))) ; returns the triples

(defn create-draft-graph!
  "Creates a new draft graph with a unique graph name, expects the
  live graph to already be created.  Returns the URI of the draft that
  was created.

  Converts the optional opts hash into drafter meta-data triples
  attached to the draft graph resource in the drafter state graph."
  ([db live-graph-uri]
     (create-draft-graph! db live-graph-uri {}))
  ([db live-graph-uri opts]
     (let [now (Date.)
           draft-graph-uri (make-draft-graph-uri)]
       ;; adds the triples returned by crate-draft-graph to the state graph
       (add db (->> (create-draft-graph live-graph-uri draft-graph-uri now opts)
                    (apply to-quads)))

       draft-graph-uri)))

(defn- escape-sparql-value [val]
  (if (string? val)
    (str "\"" val "\"")
    (str val)))

(defn- upsert-single-object-sparql [subject predicate object]
  (str "DELETE {"
       (with-state-graph
         "<" subject "> <" predicate "> ?o")
       "} INSERT {"
       (with-state-graph
         "<" subject "> <" predicate  "> " (escape-sparql-value object))
       "} WHERE {"
       (with-state-graph
         "OPTIONAL { <" subject "> <" predicate  "> ?o }")
       "}"))

(defn upsert-single-object!
  "Inserts or updates the single object for a given predicate and subject in the state graph"
  [db subject predicate object]
  (let [sparql (upsert-single-object-sparql subject predicate object)]
    (update! db sparql)))

(defn add-metadata-to-graph
  "Takes a hash-map of metadata key/value pairs and adds them as
  metadata to the graphs state graph, converting keys into drafter
  URIs as necessary.  Assumes all values are strings."
  [db graph-uri metadata]
  (doseq [[meta-name value] metadata]
    (upsert-single-object! db graph-uri meta-name value)))

(defn append-data!
  ([db draft-graph-uri triples] (append-data! db draft-graph-uri triples {}))

  ([db draft-graph-uri triples metadata]
   (add-metadata-to-graph db draft-graph-uri metadata)
   (add db draft-graph-uri triples))

  ([db draft-graph-uri format triple-stream metadata]
   (add-metadata-to-graph db draft-graph-uri metadata)
   (add db draft-graph-uri format triple-stream)))

(defn set-isPublic-query [live-graph-uri is-public]
  (upsert-single-object-sparql live-graph-uri drafter:isPublic is-public))

(defn set-isPublic! [db live-graph-uri boolean-value]
  (upsert-single-object! db live-graph-uri drafter:isPublic boolean-value))

(defn delete-graph-contents! [db graph-uri]
  (update! db (str "DROP SILENT GRAPH <" graph-uri ">"))
  (log/info (str "Deleted graph " graph-uri)))

(defn delete-draft-state-query [draft-graph-uri]
  ;; if the graph-uri is a draft graph uri,
  ;; remove the mention of this draft uri, but leave the live graph as a managed graph.
  (str "DELETE {"
       (with-state-graph
         "?live <" drafter:hasDraft "> <" draft-graph-uri "> . "
         "<" draft-graph-uri "> ?p ?o . ")
       "} WHERE {"
       (with-state-graph
         "?live <" rdf:a "> <" drafter:ManagedGraph "> ; "
         "<" drafter:hasDraft "> <" draft-graph-uri "> . "
         "<" draft-graph-uri "> ?p ?o . ")
       "}"))

(defn delete-draft-state! [db draft-graph-uri]
  (log/info "Deleting state for draft " draft-graph-uri)
  (let [query-str (delete-draft-state-query draft-graph-uri)]
    (update! db query-str)))

(defn delete-draft-graph-and-its-state!
  "Deletes graph data and the state"
  [db graph-uri]
  (delete-graph-contents! db graph-uri)
  (delete-draft-state! db graph-uri)

  (log/info (str "Deleted draft graph from state " graph-uri)))

(defn delete-graph-batched!
  "Deletes graph contents as per batch size in order to avoid blocking
  writes with a lock."
  [db graph-uri batch-size]
  (let [delete-sparql (str "DELETE  {"
                           "  GRAPH <" graph-uri "> {"
                           "    ?s ?p ?o"
                           "  }"
                           "}"
                           "WHERE {"
                           "  SELECT ?s ?p ?o WHERE"
                           "  {"
                           "    GRAPH <" graph-uri "> {"
                           "      ?s ?p ?o"
                           "    }"
                           "  }"
                           "  LIMIT " batch-size
                           "}")]
    (update! db delete-sparql)))

(defn lookup-live-graph [db draft-graph-uri]
  "Given a draft graph URI, lookup and return its live graph. Returns nil if not
  found."
  (when-let [live-uri (-> (query db
                                 (str "SELECT ?live WHERE {"
                                      (with-state-graph
                                        "?live <" rdf:a "> <" drafter:ManagedGraph "> ; "
                                        "<" drafter:hasDraft "> <" draft-graph-uri "> . ")
                                      "} LIMIT 1"))
                          first
                          (get "live"))]
    (str live-uri)))

(defn- delete-live-graph-from-state-query [live-graph-uri]
  (str "DELETE WHERE"
       "{"
       (with-state-graph
         "  ?s <" rdf:a "> <" drafter:ManagedGraph "> ."
         "  <" live-graph-uri "> ?p ?o .")
       "}"))

(defn delete-live-graph-from-state! [db live-graph-uri]
  "Delete the live managed graph from the state graph"
  (update! db (delete-live-graph-from-state-query live-graph-uri))
  (log/info (str "Deleted live graph '" live-graph-uri "'from state" )))

(defn lookup-live-graph-uri [db draft-graph-uri]
  "Given a draft graph URI, lookup and return its live graph."

  (-> (log/spy (lookup-live-graph db draft-graph-uri))
      (URIImpl.)))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (with-state-graph
                         "?live <" drafter:hasDraft "> ?draft .")
                       "}")
        res (->> (query db
                        query-str)
                 (map #(str (get % "draft")))
                 (into #{}))]
    res))

(defn- return-one-or-zero-uris
  "Helper function to check there's at most only one result and return it packed as a URIImpl.
  Raise an error if there are more than one result."
  [res]
  (if (>= 1 (count res))
    (URIImpl. (first res))
    (throw (ex-info
            "Multiple drafts were found, when only one is expected.  The context is likely too broad."
            {:error :multiple-drafts-error}))))

(defn lookup-draft-graph-uri
  "Get all the draft graph URIs.  Assumes there will be at most one
  draft found."
  [db live-graph-uri]
  (let [res (->> (query db
                        (str "SELECT ?draft WHERE {"
                             (with-state-graph
                               "<" live-graph-uri ">" " <" drafter:hasDraft "> ?draft .")
                             "} LIMIT 2"))
                 (map #(str (get % "draft")))
                 return-one-or-zero-uris)]
    (log/debug "Live->Draft mapping: " live-graph-uri " -> " res)
    res))

(defn- has-duplicates? [col]
  (not= col
        (distinct col)))

(defn graph-map
  "Takes a database and a set of drafts and returns a hashmap of live
  graphs associated with their drafts.

  If a draft URI is not in the database then this function will skip
  over it, i.e. it won't error if a URI is not found."
  [db draft-set]
  (if (empty? draft-set)
    {}
    (let [drafts (apply str (map #(str "<" % "> ") draft-set))
          results (->> (query db
                              (str "SELECT ?live ?draft WHERE {"
                                   (with-state-graph
                                     "  VALUES ?draft {" drafts "}"
                                     "  ?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                                     "        <" drafter:hasDraft "> ?draft .")
                                   "}")))]
      (let [live-graphs (map #(get % "live") results)]
        (when (has-duplicates? live-graphs)
          (throw (ex-info "Multiple draft graphs were supplied referencing the same live graph."
                          {:error :multiple-drafts-error})))

        (zipmap live-graphs
                (map #(get % "draft") results))))))

(defn live-graphs [db & {:keys [online] :or {online true}}]
  "Get all live graph names.  Takes an optional boolean keyword
  argument of :online to allow querying for all online/offline live
  graphs."
  (->> (query db
                 (str "SELECT ?live WHERE {"
                      (with-state-graph
                        "?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                        "<" drafter:isPublic  "> " online " .")
                      "}"))
          (map #(str (% "live")))
          (into #{})))

(defn- move-like-tbl-wants-super-slow-on-stardog-though
  "Move's how TBL intended.  Issues a SPARQL MOVE query.

  Note this is super slow on stardog 3.1."
  [source destination]
  ;; Move's how TBL intended...
  (str "MOVE SILENT <" source "> TO <" destination ">"))

(defn- delete-insert-move
  "Move source graph to destination.  Semantically the same as MOVE but done
  with DELETE/INSERT's.

  Massively quicker  on stardog than a MOVE."
  [source destination]
  (str
   ;; first clear the destination, then...
   "DELETE {"
   "  GRAPH <" destination "> {?s ?p ?o}"
   "} WHERE {"
   "  GRAPH <" destination "> {?s ?p ?o} "
   "};"
   ;; copy the source to the destination, and...
   "INSERT {"
   "  GRAPH <" destination "> {?s ?p ?o}"
   "} WHERE { "
   "  GRAPH <" source "> {?s ?p ?o}"
   "};"
   ;; remove the source (draft) graph
   "DELETE {"
   "  GRAPH <" source "> {?s ?p ?o}"
   "} WHERE {"
   " GRAPH <" source "> {?s ?p ?o}"
   "}"))

(defn graph-non-empty-query [graph-uri]
  (str
   "ASK WHERE {
    SELECT * WHERE {
      GRAPH <" graph-uri "> { ?s ?p ?o }
    } LIMIT 1
  }"))

(defn graph-non-empty?
  "Returns true if the graph contains any statements."
  [repo graph-uri]
  (query repo (graph-non-empty-query graph-uri)))

(defn graph-empty?
  "Returns true if there are no statements in the associated graph."
  [repo graph-uri]
  (not (graph-non-empty? repo graph-uri)))

(defn should-delete-live-graph-from-state-after-draft-migrate?
  "When migrating a draft graph to live, the associated 'is managed
  graph' statement should be removed from the state if graph if:
  1. The migrate operation is a delete (i.e. the draft graph is empty)
  2. The migrated graph is the only draft associated with the live
  graph."
  [repo draft-graph-uri live-graph-uri]
  (and
   (graph-empty? repo draft-graph-uri)
   (not (has-more-than-one-draft? repo live-graph-uri))))

;;migrate-live-queries :: Repository -> String -> { queries: [String], live-graph-uri: String }
(defn migrate-live-queries [db draft-graph-uri]
  (if-let [live-graph-uri (lookup-live-graph db draft-graph-uri)]
    (let [move-query (delete-insert-move draft-graph-uri live-graph-uri)
          delete-state-query (delete-draft-state-query draft-graph-uri)
          live-public-query (set-isPublic-query live-graph-uri true)
          queries [move-query delete-state-query live-public-query]
          queries (if (should-delete-live-graph-from-state-after-draft-migrate? db draft-graph-uri live-graph-uri)
                    (conj queries (delete-live-graph-from-state-query live-graph-uri))
                    queries)]
      {:queries queries
       :live-graph-uri live-graph-uri})

    (throw (ex-info (str "Could not find the live graph associated with graph " draft-graph-uri)
                    {:error :graph-not-found}))))

(defn migrate-live!
  "Moves the triples from the draft graph to the draft graphs live destination."
  [db draft-graph-uri]
  (log/debug (str "Migrating graph: " draft-graph-uri " to live"))
  (let [{:keys [queries live-graph-uri]} (migrate-live-queries db draft-graph-uri)
        query-str (util/make-compound-sparql-query queries)]
    (update! db query-str)
    (log/info (str "Migrated graph: " draft-graph-uri " to live graph " live-graph-uri))))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  [db graph triples]

  (create-managed-graph! db graph)
  (let [draft-graph (create-draft-graph! db graph)]
    (add db draft-graph triples)
    draft-graph))
