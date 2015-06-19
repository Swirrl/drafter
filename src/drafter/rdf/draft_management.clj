(ns drafter.rdf.draft-management
  (:require [clojure.tools.logging :as log]
            [grafter.rdf :refer [add s]]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.repository :refer [query]]
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

(defn set-isPublic! [db live-graph-uri boolean-value]
  (upsert-single-object! db live-graph-uri drafter:isPublic boolean-value))

(defn delete-graph-contents! [db graph-uri]
  (update! db (str "DROP GRAPH <" graph-uri ">"))
  (log/info (str "Deleted graph " graph-uri)))

(defn delete-draft-graph-state! [db graph-uri]
  (let [query-str (str "DELETE {"
                       (with-state-graph
                         "?live <" drafter:hasDraft "> <" graph-uri "> . "
                         "<" graph-uri "> ?p ?o . ")
                       "} WHERE {"
                       (with-state-graph
                         "?live <" rdf:a "> <" drafter:ManagedGraph "> ; "
                               "<" drafter:hasDraft "> <" graph-uri "> . "
                               "<" graph-uri "> ?p ?o . ")
                       "}")]

    ;; if the graph-uri is a draft graph uri, remove the mention of
    ;; this draft uri, but leave the live graph as a managed graph.
    (update! db query-str)
    (log/info (str "Deleted draft graph from state " graph-uri))))

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

(defn- delete-live-graph-from-state [db live-graph-uri]
  (str "DELETE WHERE"
       "{"
       (with-state-graph
         "<" live-graph-uri "> a <" drafter:ManagedGraph "> ;"
         "   ?p ?o .")
       "}"))

(defn delete-live-graph-from-state! [db live-graph-uri]
  "Delete the live managed graph from the state graph"
  (update! db (delete-live-graph-from-state db live-graph-uri))
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
              (map #(get % "draft") results)))))

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

(defn migrate-live!
  "Moves the triples from the draft graph to the draft graphs live destination."
  [db draft-graph-uri]

  (if-let [live-graph-uri (lookup-live-graph db draft-graph-uri)]
    (do
      ;;DELETE the target (live) graph and copy the draft to it
      ;;TODO: Use MOVE query?
      (delete-graph-contents! db live-graph-uri)

      (let [contents (query db
                            (str "CONSTRUCT { ?s ?p ?o } WHERE
                                 { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }"))

            ;;If the source (draft) graph is empty then the migration
            ;;is a deletion. If it is the only draft of the live graph
            ;;then all references to the live graph are being removed
            ;;from the data. In this case the reference to the live
            ;;graph should be removed from the state graph. Note this
            ;;case and use it when cleaning up the state graph below.
            is-only-draft? (not (has-more-than-one-draft? db live-graph-uri))]

        ;;if the source (draft) graph has data then copy it to the live graph and
        ;;make it public.
        (if (not (empty? contents))
          (do
            (add db live-graph-uri contents)
            (set-isPublic! db live-graph-uri true)))

        ;;delete draft data
        (delete-graph-contents! db draft-graph-uri)

        ;;NOTE: At this point all the live and draft graph data has
        ;;been updated: the live graph contents match those of the
        ;;published draft, and the draft data has been deleted.

        ;;Clean up the state graph - all references to the draft graph should always be removed.
        ;;The live graph should be removed if the draft was empty (operation was a deletion) and
        ;;it was the only draft of the live graph

        ;;WARNING: Draft graph state must be deleted before the live graph state!
        ;;DELETE query depends on the existence of the live->draft connection in the state
        ;;graph
        (delete-draft-graph-state! db draft-graph-uri)

        (if (and is-only-draft? (empty? contents))
          (delete-live-graph-from-state! db live-graph-uri)))
      
      (log/info (str "Migrated graph: " draft-graph-uri " to live graph: " live-graph-uri)))

    (throw (ex-info (str "Could not find the live graph associated with graph " draft-graph-uri)
                    {:error :graph-not-found}))))

(defn import-data-to-draft!
  "Imports the data from the triples into a draft graph associated
  with the specified graph.  Returns the draft graph uri."
  [db graph triples]

  (create-managed-graph! db graph)
  (let [draft-graph (create-draft-graph! db graph)]
    (add db draft-graph triples)
    draft-graph))
