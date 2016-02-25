(ns drafter.rdf.draft-management
  (:require [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter.rdf :refer [add s]]
            [drafter.util :refer [map-values]]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.set :as set]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.repository :refer [query]]
            [drafter.backend.protocols :refer [migrate-graphs-to-live!]]
            [grafter.rdf.templater :refer [add-properties graph]])
  (:import (java.util Date UUID)
           (java.net URI)
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

(defn is-graph-live? [db graph-uri]
  (query db
         (str "ASK WHERE {"
              (with-state-graph
                "<" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ."
                "<" graph-uri "> <" drafter:isPublic "> true ."
                "}"))))

(defn draft-exists?
  "Checks state graph to see if a draft graph exists"
  [db graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT ?s WHERE {"
                 (with-state-graph
                 "      ?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                 "        <" drafter:hasDraft "> <" graph-uri "> ."
                 "        <" graph-uri "> a <" drafter:DraftGraph "> ."
                 "  }")
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
   (create-draft-graph live-graph-uri draft-graph-uri time opts nil))
  ([live-graph-uri draft-graph-uri time opts draftset-uri]

     (let [live-graph-triples [live-graph-uri
                               [drafter:hasDraft draft-graph-uri]]
           draft-graph-triples  [draft-graph-uri
                                 [rdf:a drafter:DraftGraph]
                                 [drafter:modifiedAt time]]
           draft-graph-triples (util/conj-if (some? draftset-uri) draft-graph-triples [drafter:inDraftSet draftset-uri])
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
  attached to the draft graph resource in the drafter state graph.

  If draftset-uri is provided (i.e. is not nil) a statement will be
  added connecting the created draft graph to the given draft set. No
  validation is done that the draftset actually exists."
  ([db live-graph-uri]
   (create-draft-graph! db live-graph-uri {}))
  ([db live-graph-uri opts]
   (create-draft-graph! db live-graph-uri opts nil))
  ([db live-graph-uri opts draftset-uri]
     (let [now (Date.)
           draft-graph-uri (make-draft-graph-uri)]
       ;; adds the triples returned by crate-draft-graph to the state graph
       (add db (->> (create-draft-graph live-graph-uri draft-graph-uri now opts draftset-uri)
                    (apply to-quads)))

       draft-graph-uri)))

(defn ensure-draft-exists-for [repo live-graph graph-map draftset-uri]
  (if-let [draft-graph (get graph-map live-graph)]
    {:draft-graph-uri draft-graph :graph-map graph-map}
    (let [live-graph-uri (create-managed-graph! repo live-graph)
          draft-graph-uri (create-draft-graph! repo live-graph-uri {} draftset-uri)]
      {:draft-graph-uri draft-graph-uri :graph-map (assoc graph-map live-graph-uri draft-graph-uri)})))

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

(defn delete-draft-graph-state! [db draft-graph-uri]
  (log/info "Deleting state for draft " draft-graph-uri)
  (let [query-str (delete-draft-state-query draft-graph-uri)]
    (update! db query-str)

    ;; if the graph-uri is a draft graph uri, remove the mention of
    ;; this draft uri, but leave the live graph as a managed graph.
    (log/info (str "Deleted draft graph from state "draft-graph-uri))))

(defn delete-draft-graph-and-remove-from-state-query [draft-graph-uri]
  (let [drop-query (format "DROP SILENT GRAPH <%s>" draft-graph-uri)
        delete-from-state-query (delete-draft-state-query draft-graph-uri)]
    (util/make-compound-sparql-query [drop-query delete-from-state-query])))

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

(defn- delete-dependent-private-managed-graph-query [draft-graph-uri]
  (str
   "DELETE {"
   (with-state-graph
     "?lg ?lp ?lo .")
   "} WHERE {"
   (with-state-graph
     "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
     "?lg <" drafter:isPublic "> false ."
     "?lg ?lp ?lo ."
     "FILTER NOT EXISTS {"
     "  ?lg <" drafter:hasDraft "> ?odg ."
     "  FILTER (?odg != <" draft-graph-uri ">)"
     "}")
   "}"))

(defn- delete-draft-graph-query [draft-graph-uri]
  (util/make-compound-sparql-query
   [(delete-draft-graph-and-remove-from-state-query draft-graph-uri)
    (delete-dependent-private-managed-graph-query draft-graph-uri)]))

(defn delete-draft-graph!
  "Deletes a draft graph's contents and all references to it in the
  state graph. If its associated managed graph is private and has only
  the given draft graph then it will also be removed."
  [db draft-graph-uri]
  (update! db (delete-draft-graph-query draft-graph-uri)))

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

(defn get-live-graph-for-draft
  "Gets the live graph URI corresponding to a draft graph. Returns nil
  if the draft URI does not have an associated managed graph or if the
  live graph does not exist."
  [db draft-graph-uri]
  (if (draft-exists? db draft-graph-uri)
    (lookup-live-graph db draft-graph-uri)))

(defn delete-live-graph-from-state-query [live-graph-uri]
  (str "DELETE WHERE"
       "{"
       (with-state-graph
         "<" live-graph-uri "> a <" drafter:ManagedGraph "> ;"
         "   ?p ?o .")
       "}"))

(defn delete-live-graph-from-state! [db live-graph-uri]
  "Delete the live managed graph from the state graph"
  (update! db (delete-live-graph-from-state-query live-graph-uri))
  (log/info (str "Deleted live graph '" live-graph-uri "'from state" )))

(defn- delete-empty-draft-graphs-query []
  (str "DELETE {"
       (with-state-graph
         "?dg ?sp ?so ."
         "?lg <" drafter:hasDraft "> ?dg .")
       "} WHERE {"
       (with-state-graph
         "?dg <" rdf:a "> <" drafter:DraftGraph "> ."
         "?dg ?dp ?do ."
         "OPTIONAL {"
         "  ?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
         "  ?lg <" drafter:hasDraft "> ?dg ."
         "}")
       "  FILTER NOT EXISTS { GRAPH ?dg { ?s ?p ?o } }"
       "}"))

(defn delete-empty-draft-graphs! [db]
  (update! db (delete-empty-draft-graphs-query)))

(defn- delete-private-managed-graphs-without-drafts-query []
  (str
   "DELETE { "
   (with-state-graph
     "?lg ?p ?o .")
   "} WHERE { "
   (with-state-graph
     "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
     "?lg <" drafter:isPublic "> false ."
     "?lg ?p ?o .")
   "FILTER NOT EXISTS {"
   "  ?dg <" rdf:a "> <" drafter:DraftGraph "> ."
   "  ?lg <" drafter:hasDraft "> ?dg ."
   "}"
   "}"))

(defn delete-private-managed-graphs-without-drafts! [db]
  (update! db (delete-private-managed-graphs-without-drafts-query)))

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

(defn- parse-guid [uri]
  (.replace (str uri) (draft-uri "") ""))

(def ^:private get-all-drafts-query (str
                           "SELECT ?draft ?live WHERE {"
                           "   GRAPH <" drafter-state-graph "> {"
                           "     ?draft a <" drafter:DraftGraph "> . "
                           "     ?live <" drafter:hasDraft "> ?draft . "
                           "   }"
                           "}"))

;;SPARQLable -> Map{Keyword String}
(defn query-all-drafts [queryable]
  (doall (->> (query queryable get-all-drafts-query)
                (map keywordize-keys)
                (map (partial map-values str))
                (map (fn [m] (assoc m :guid (parse-guid (:draft m))))))))

(defn migrate-live! [backend graph]
  (migrate-graphs-to-live! backend [graph]))

(defn calculate-graph-restriction [public-live-graphs live-graph-drafts supplied-draft-graphs]
  (set/union
   (set/difference public-live-graphs live-graph-drafts)
   supplied-draft-graphs))

(defn graph-mapping->graph-restriction [db graph-mapping union-with-live?]
  (let [live-graphs (if union-with-live? (live-graphs db) #{})]
    (calculate-graph-restriction live-graphs (keys graph-mapping) (vals graph-mapping))))
