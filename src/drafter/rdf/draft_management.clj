(ns drafter.rdf.draft-management
  (:require [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter.rdf :refer [add s]]
            [drafter.util :refer [map-values]]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.set :as set]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.repository :as repo]
            [drafter.backend.protocols :refer [migrate-graphs-to-live!]]
            [drafter.backend.sesame.common.protocols :refer [->repo-connection]]
            [grafter.rdf.templater :refer [add-properties graph]]
            [swirrl-server.errors :refer [ex-swirrl]]
            [schema.core :as s])
  (:import (java.util Date UUID)
           (java.net URI)
           (org.openrdf.model.impl URIImpl)))

(def drafter-state-graph "http://publishmydata.com/graphs/drafter/drafts")

(def staging-base "http://publishmydata.com/graphs/drafter/draft")

(def to-quads (partial graph drafter-state-graph))

(def prefixes (str "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX drafter: <" (drafter "") ">"))


(defn- mapply [f & args]
  (apply f (apply concat (butlast args) (last args))))

(defn query [repo query-str & {:as opts}]
  (mapply repo/query repo (str prefixes query-str) opts))

(defn update! [repo update-string]
  (let [update-string (str prefixes update-string)]
    (log/info "Running update: " update-string)
    (pr/update! repo update-string)))

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
                "<" graph-uri "> a drafter:ManagedGraph ."
                "}")
              )))

(defn is-graph-live? [db graph-uri]
  (query db
         (str "ASK WHERE {"
              (with-state-graph
                "   <" graph-uri "> a drafter:ManagedGraph ."
                "   <" graph-uri "> drafter:isPublic true ."
                "}"))))

(defn draft-exists?
  "Checks state graph to see if a draft graph exists"
  [db graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT ?s WHERE {"
                 (with-state-graph
                 "      ?live a drafter:ManagedGraph ;"
                 "           drafter:hasDraft <" graph-uri "> ."
                 "      <" graph-uri "> a drafter:DraftGraph ."
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
                 "      <" live-graph-uri "> a drafter:ManagedGraph ;"
                 "                 drafter:hasDraft ?draft ."
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
                                 [drafter:createdAt time]
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
   (create-draft-graph! db live-graph-uri opts draftset-uri (java.util.Date.)))
  ([db live-graph-uri opts draftset-uri now]
   (let [draft-graph-uri (make-draft-graph-uri)]
     ;; adds the triples returned by crate-draft-graph to the state graph
     (add db (->> (create-draft-graph live-graph-uri draft-graph-uri now opts draftset-uri)
                  (apply to-quads)))

     draft-graph-uri)))

(defn xsd-datetime
  "Coerce a date into the xsd-datetime string"
  [datetime]
  (let [date-as-calendar (doto (java.util.Calendar/getInstance)
                           (.setTime datetime))
        instant (javax.xml.bind.DatatypeConverter/printDateTime date-as-calendar)]

    (str "\"" instant "\"^^xsd:dateTime")))

(defn- set-timestamp [subject class-uri time-predicate datetime]
  (let [instant (xsd-datetime datetime)]
    (str
     "WITH <http://publishmydata.com/graphs/drafter/drafts>"
     "DELETE {"
     "   <" subject "> <" time-predicate "> ?lastvalue ."
     "}"
     "INSERT { "
     "   <" subject "> <" time-predicate "> " instant " ."
     "}"
     "WHERE {"
     "   <" subject "> a <" class-uri "> ."
     "  OPTIONAL {"
     "     <" subject "> <" time-predicate "> ?lastvalue ."
     "  }"
     "}")))

(s/defn set-timestamp-on-instance-of-class!
  "Sets the specified object on the specified subject/predicate.  It
  assumes the property has a cardinality of 1 or 0, so will delete all
  other values of \":subject :predicate ?object\" if present."
  [class-uri
   predicate
   repo
   subject
   date-time :- Date]

  (update! repo (set-timestamp subject class-uri predicate date-time)))

(def ^{:doc "Set modified at time on a draft graph.  It is assumed the
  cardinality of modifiedAt is at most 1, and that it will be updated in
  place."}  set-modifed-at-on-draft-graph!
  (partial set-timestamp-on-instance-of-class! drafter:DraftGraph drafter:modifiedAt))

(defn ensure-draft-exists-for
  [repo live-graph graph-map draftset-uri]
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
  (str
   "WITH <http://publishmydata.com/graphs/drafter/drafts> "
   "DELETE {"
   "   <" subject "> <" predicate "> ?o ."
   "} INSERT {"
   "   <" subject "> <" predicate  "> " (escape-sparql-value object) " . "
   "} WHERE {"
   "   OPTIONAL { <" subject "> <" predicate  "> ?o }"
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
  (str
   "WITH <http://publishmydata.com/graphs/drafter/drafts>"
   "DELETE {"
   "   ?live drafter:hasDraft <" draft-graph-uri "> ."
   "   <" draft-graph-uri "> ?p ?o ."
   "} WHERE {"
   "   ?live a drafter:ManagedGraph ;"
   "         <" drafter:hasDraft "> <" draft-graph-uri "> ."
   "   <" draft-graph-uri "> ?p ?o . "
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

(defn- delete-dependent-private-managed-graph-query [draft-graph-uri]
  (str
   "WITH <http://publishmydata.com/graphs/drafter/drafts>"
   "DELETE {"
   "   ?lg ?lp ?lo ."
   "} WHERE {"
   "   ?lg a drafter:ManagedGraph ."
   "   ?lg drafter:isPublic false ."
   "   ?lg ?lp ?lo ."
   "   FILTER NOT EXISTS {"
   "      ?lg drafter:hasDraft ?odg ."
   "      FILTER (?odg != <" draft-graph-uri ">)"
   "   }"
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
                                        "?live a drafter:ManagedGraph ; "
                                        "      drafter:hasDraft <" draft-graph-uri "> . ")
                                      "} LIMIT 1"))
                          first
                          (get "live"))]
    (str live-uri)))

(defn delete-live-graph-from-state-query [live-graph-uri]
  (str
   "DELETE WHERE {"
   "GRAPH <http://publishmydata.com/graphs/drafter/drafts> {"
   "<" live-graph-uri "> a drafter:ManagedGraph ;"
   "   ?p ?o ."
   "}"
   "}"))

(defn delete-live-graph-from-state! [db live-graph-uri]
  "Delete the live managed graph from the state graph"
  (update! db (delete-live-graph-from-state-query live-graph-uri))
  (log/info (str "Deleted live graph '" live-graph-uri "'from state" )))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (with-state-graph
                         "?live drafter:hasDraft ?draft .")
                       "}")
        res (->> (query db
                        query-str)
                 (map #(str (get % "draft")))
                 (into #{}))]
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
    (let [drafts (clojure.string/join " " (map #(str "<" % ">") draft-set))
          results (->> (query db
                              (str "SELECT ?live ?draft WHERE {"
                                   (with-state-graph
                                     "  VALUES ?draft {" drafts "}"
                                     "  ?live a drafter:ManagedGraph ;"
                                     "        drafter:hasDraft ?draft .")
                                   "}")))]
      (let [live-graphs (map #(get % "live") results)]
        (when (has-duplicates? live-graphs)
          (throw (ex-swirrl :multiple-drafts-error
                            "Multiple draft graphs were supplied referencing the same live graph.")))

        (zipmap live-graphs
                (map #(get % "draft") results))))))

(defn live-graphs [db & {:keys [online] :or {online true}}]
  "Get all live graph names.  Takes an optional boolean keyword
  argument of :online to allow querying for all online/offline live
  graphs."
  (->> (query db
                 (str "SELECT ?live WHERE {"
                      (with-state-graph
                        "?live a drafter:ManagedGraph ;"
                        "      drafter:isPublic " online " .")
                      "}"))
          (map #(str (% "live")))
          (into #{})))

(defn- parse-guid [uri]
  (.replace (str uri) (draft-uri "") ""))

(def ^:private get-all-drafts-query (str
                           "SELECT ?draft ?live WHERE {"
                           "   GRAPH <" drafter-state-graph "> {"
                           "     ?draft a drafter:DraftGraph . "
                           "     ?live drafter:hasDraft ?draft . "
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

(defn append-data-batch!
  "Appends a sequence of triples to the given draft graph."
  [backend graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (if-not (empty? triple-batch)
    ;;WARNING: This assumes the backend is a sesame backend which is
    ;;true for all current backends.
    (with-open [conn (->repo-connection backend)]
      (repo/with-transaction conn
        (add conn graph-uri triple-batch)))))
