(ns drafter.backend.draftset.draft-management
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [drafter.backend.common :refer [->repo-connection ->sesame-repo]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql :refer [update!]]
            [drafter.util :as util]
            [grafter-2.rdf4j.templater :refer [add-properties graph]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter.url :as url]
            [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:modified]]
            [grafter.vocabularies.rdf :refer :all]
            [schema.core :as s]
            [grafter-2.rdf4j.io :as rio]
            [clojure.string :as string]
            [drafter.draftset :as ds])
  (:import java.net.URI
           [java.util Date UUID Calendar]
           [javax.xml.bind DatatypeConverter]))

(def drafter-state-graph (URI. "http://publishmydata.com/graphs/drafter/drafts"))

(def staging-base (URI. "http://publishmydata.com/graphs/drafter/draft"))

(def to-quads (partial graph drafter-state-graph))

(defn make-draft-graph-uri []
  (url/->java-uri (url/append-path-segments staging-base (str (UUID/randomUUID)))))

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
  (sparql/eager-query db
         (str "ASK WHERE {"
              (with-state-graph
                "<" graph-uri "> a <" drafter:ManagedGraph "> ."
                "}"))))

(defn is-graph-live? [db graph-uri]
  (sparql/eager-query db
         (str "ASK WHERE {"
              (with-state-graph
                "   <" graph-uri "> a <" drafter:ManagedGraph "> ."
                "   <" graph-uri "> <" drafter:isPublic "> true ."
                "}"))))

(defn has-more-than-one-draft?
  "Given a live graph uri, check to see if it is referenced by more
  than one draft in the state graph."
  [db live-graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT (COUNT(?draft) AS ?numberOfRefs)   WHERE {"
                 "    {"
                 "      <" live-graph-uri "> a <" drafter:ManagedGraph "> ;"
                 "                           <" drafter:hasDraft "> ?draft ."
                 "    }"
                 "  }"
                 "  HAVING (?numberOfRefs > 1)"
                 "}")]
    (sparql/eager-query db qry)))

(defn graph-exists?
  "Checks that a graph exists"
  [db graph-uri]
  (sparql/eager-query db
         (str "ASK WHERE {"
              "  SELECT ?s ?p ?o WHERE {"
              "    GRAPH <" graph-uri "> { ?s ?p ?o }"
              "  }"
              "  LIMIT 1"
              "}")))

(defn create-managed-graph
  "Returns some RDF statements to represent the ManagedGraphs state."
  [graph-uri]
  [graph-uri
   [rdf:a drafter:ManagedGraph]
   [drafter:isPublic false]])

(defn create-managed-graph!
  "Create a record of a managed graph in the database, returns the
  graph-uri that was passed in."
  [db graph-uri]
  ;; We only do anything if it's not already a managed graph

  ;; FIXME: Is this a potential race condition? i.e. we check for existence (and it's false) and before executing someone else makes
  ;; the managed graph(?). Ideally, we'd do this as a single INSERT/WHERE statement.
  (if (not (is-graph-managed? db graph-uri))
    (let [managed-graph-quads (to-quads (create-managed-graph graph-uri))]
      (sparql/add db managed-graph-quads)))
  graph-uri)

(defn create-draft-graph
  [live-graph-uri draft-graph-uri time draftset-uri]
  (let [live-graph-triples [live-graph-uri
                            [drafter:hasDraft draft-graph-uri]]
        draft-graph-triples [draft-graph-uri
                             [rdf:a drafter:DraftGraph]
                             [drafter:createdAt time]
                             [drafter:modifiedAt time]]
        draft-graph-triples (util/conj-if (some? draftset-uri)
                                          draft-graph-triples [drafter:inDraftSet draftset-uri])]
    [live-graph-triples draft-graph-triples]))

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
   (create-draft-graph! db live-graph-uri nil))
  ([db live-graph-uri draftset-ref]
   (create-draft-graph! db live-graph-uri draftset-ref util/get-current-time))
  ([db live-graph-uri draftset-ref clock-fn]
   (let [now (clock-fn)
         draft-graph-uri (make-draft-graph-uri)
         draftset-uri (some-> draftset-ref (url/->java-uri))
         triple-templates (create-draft-graph live-graph-uri draft-graph-uri now draftset-uri)
         quads (apply to-quads triple-templates)]
     (sparql/add db quads)

     draft-graph-uri)))

(defn xsd-datetime
  "Coerce a date into the xsd-datetime string"
  [offset-datetime]
  (str (rio/->backend-type offset-datetime)))

(defn set-timestamp
  "Returns an update string to update the given subject/resource with
  the supplied a timestamp."
  [subject time-predicate datetime]
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
     "   <" subject "> ?p ?o . "
     "  OPTIONAL {"
     "     <" subject "> <" time-predicate "> ?lastvalue ."
     "  }"
     "}")))

(s/defn set-timestamp-on-resource!
  "Sets the specified object on the specified subject/predicate.  It
  assumes the property has a cardinality of 1 or 0, so will delete all
  other values of \":subject :predicate ?object\" if present."
  [predicate
   repo
   subject
   date-time :- Date]

  (update! repo (set-timestamp subject predicate date-time)))

(def ^{:doc "Set modified at time on a resource.  It is assumed the
  cardinality of modifiedAt is at most 1, and that it will be updated in
  place."}  set-modifed-at-on-resource!
  (partial set-timestamp-on-resource! drafter:modifiedAt))

(defn ensure-draft-exists-for
  "Finds or creates a draft graph for the given live graph in the
  draftset."
  [repo live-graph graph-map draftset-uri]
  (if-let [draft-graph (get graph-map live-graph)]
    {:draft-graph-uri draft-graph :graph-map graph-map}
    (let [live-graph-uri (create-managed-graph! repo live-graph)
          draft-graph-uri (create-draft-graph! repo live-graph-uri draftset-uri)]
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

(defn delete-graph-contents-query [graph-uri]
  (str "DROP SILENT GRAPH <" graph-uri ">"))

(defn delete-graph-contents!
  "Transactionally delete the contents of the supplied graph and set
  its modified time to the supplied instant.

  Note modified-at is an instant not a 0-arg clock-fn."
  [db graph-uri modified-at]
  (update! db (str (delete-graph-contents-query graph-uri) " ; "
                   (set-timestamp graph-uri drafter:modifiedAt modified-at)))
  (log/info (str "Deleted graph " graph-uri)))

(defn delete-draft-state-query [draft-graph-uri]
  ;; if the graph-uri is a draft graph uri,
  ;; remove the mention of this draft uri, but leave the live graph as a managed graph.
  (str
   "WITH <" drafter-state-graph ">"
   "DELETE {"
   "   ?live <" drafter:hasDraft "> <" draft-graph-uri "> ."
   "   <" draft-graph-uri "> ?p ?o ."
   "} WHERE {"
   "   ?live a <" drafter:ManagedGraph "> ;"
   "         <" drafter:hasDraft "> <" draft-graph-uri "> ."
   "   <" draft-graph-uri "> ?p ?o . "
   "}"))

(defn delete-draft-graph-and-remove-from-state-query [draft-graph-uri]
  (let [drop-query (format "DROP SILENT GRAPH <%s>" draft-graph-uri)
        delete-from-state-query (delete-draft-state-query draft-graph-uri)]
    (util/make-compound-sparql-query [drop-query delete-from-state-query])))

(defn- delete-dependent-private-managed-graph-query [draft-graph-uri]
  (str
   "WITH <" drafter-state-graph ">"
   "DELETE {"
   "   ?lg ?lp ?lo ."
   "} WHERE {"
   "   ?lg a <" drafter:ManagedGraph "> ."
   "   ?lg <" drafter:isPublic "> false ."
   "   ?lg ?lp ?lo ."
   "   MINUS {"
   "      ?lg <" drafter:hasDraft "> ?odg ."
   "      FILTER (?odg != <" draft-graph-uri ">)"
   "   }"
   "}"))

(defn- delete-draft-graph-query [draft-graph-uri]
  (let [q (util/make-compound-sparql-query
           [(delete-draft-graph-and-remove-from-state-query draft-graph-uri)
            (delete-dependent-private-managed-graph-query draft-graph-uri)])]
    q))

(defn delete-draft-graph!
  "Deletes a draft graph's contents and all references to it in the
  state graph. If its associated managed graph is private and has only
  the given draft graph then it will also be removed."
  [db draft-graph-uri]
  (update! db (delete-draft-graph-query draft-graph-uri)))

(defn lookup-live-graph [db draft-graph-uri]
  "Given a draft graph URI, lookup and return its live graph. Returns nil if not
  found."
  (let [q (str "SELECT ?live WHERE {"
               (with-state-graph
                 "?live a <" drafter:ManagedGraph "> ;"
                 "      <" drafter:hasDraft "> <" draft-graph-uri "> . ")
               "} LIMIT 1")]
    (-> (sparql/eager-query db q)
        first
        (:live))))

(defn delete-live-graph-from-state-query [live-graph-uri]
  (str
   "DELETE WHERE {"
   (with-state-graph
     "<" live-graph-uri "> a <" drafter:ManagedGraph "> ;"
     "                     ?p ?o")
   "}"))

(defn copy-graph-query
  [from to {:keys [silent] :as opts :or {silent false}}]
  (let [silent (if silent
                 "SILENT"
                 "")]
    (str
     "\n"
     "COPY " silent " <" from "> TO <" to ">")))

(defn copy-graph
  "Copies source graph to destination graph.  Accepts an optional map
  of options.

  Currently the only supported option is the boolean value :silent
  which will ensure the copy always succeeds, whether or not their is
  a source graph etc."
  ([repo from to] (copy-graph repo from to {}))
  ([repo from to opts]
   (update! repo (copy-graph-query from to opts))))

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
    (let [drafts (str/join " " (map #(str "<" % ">") draft-set))
          q (str "SELECT ?live ?draft WHERE {"
                (with-state-graph
                  "  VALUES ?draft {" drafts "}"
                  "  ?live a <" drafter:ManagedGraph "> ;"
                  "        <" drafter:hasDraft "> ?draft .")
                "}")
          results (sparql/eager-query db q)]

      (let [live-graphs (map :live results)]
        (when (has-duplicates? live-graphs)
          (throw (ex-info "Multiple draft graphs were supplied referencing the same live graph."
                          {:error :multiple-drafts-error})))

        (zipmap live-graphs
                (map :draft results))))))

(defn live-graphs
  "Get all live graph names.  Takes an optional boolean keyword
  argument of :online to allow querying for all online/offline live
  graphs."
  [db & {:keys [online] :or {online true}}]
  (let [q (str "SELECT ?live WHERE {"
               (with-state-graph
                 "?live a <" drafter:ManagedGraph "> ;"
                 "      <" drafter:isPublic "> " online " .")
               "}")
        results (sparql/eager-query db q)]
    (into #{} (map :live results))))

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
  (sparql/eager-query repo (graph-non-empty-query graph-uri)))

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

(defn- update-live-graph-timestamps-query [draft-graph-uri now-ts]
  (let [issued-at (xsd-datetime now-ts)]
    (str
      "# First remove the modified timestamp from the live graph\n"
      "WITH <" drafter-state-graph "> DELETE {"
      "  ?live <" dcterms:modified "> ?modified ."
      "} WHERE {"
      "  VALUES ?draft { <" draft-graph-uri "> }"
      "  ?live a <" drafter:ManagedGraph "> ;"
      "        <" drafter:hasDraft "> ?draft ;"
      "        <" dcterms:modified "> ?modified ."
      "  ?draft a <" drafter:DraftGraph "> ."
      "};\n"

      "# Then set the modified timestamp on the live graph to be that of the draft graph\n"
      "WITH <" drafter-state-graph "> INSERT {"
      "  ?live <" dcterms:modified "> ?modified ."
      "} WHERE {"
      "  VALUES ?draft { <" draft-graph-uri "> }"
      "  ?live a <" drafter:ManagedGraph "> ;"
      "        <" drafter:hasDraft "> ?draft ."
      "  ?draft a <" drafter:DraftGraph "> ;"
      "         <" dcterms:modified "> ?modified ."
      "};\n"

      "# And finally set a dcterms:issued timestamp if it doesn't have one already\n"
      "WITH <" drafter-state-graph "> INSERT {"
      "  ?live <" dcterms:issued "> " issued-at " ."
      "} WHERE {"
      "  VALUES ?draft { <" draft-graph-uri "> }"
      "  ?live a <" drafter:ManagedGraph "> ;"
      "        <" drafter:hasDraft "> ?draft ."
      "  FILTER NOT EXISTS { ?live <" dcterms:issued "> ?existing . }"
      "}")))

(defn- move-graph
  "Move's how TBL intended.  Issues a SPARQL MOVE query.
  Note this is super slow on stardog 3.1."
  [source destination]
  ;; Move's how TBL intended...
  (str "MOVE SILENT <" source "> TO <" destination ">"))

;;Repository -> String -> { queries: [String], live-graph-uri: String }
(defn- migrate-live-queries [db draft-graph-uri transaction-at]
  (if-let [live-graph-uri (lookup-live-graph db draft-graph-uri)]
    (let [move-query (move-graph draft-graph-uri live-graph-uri)
          update-timestamps-query (update-live-graph-timestamps-query draft-graph-uri transaction-at)
          delete-state-query (delete-draft-state-query draft-graph-uri)
          live-public-query (set-isPublic-query live-graph-uri true)
          queries [update-timestamps-query move-query delete-state-query live-public-query]
          queries (if (should-delete-live-graph-from-state-after-draft-migrate? db draft-graph-uri live-graph-uri)
                    (conj queries (delete-live-graph-from-state-query live-graph-uri))
                    queries)]
      {:queries queries
       :live-graph-uri live-graph-uri})))

(defn- rewrite-1-q
  [{:keys [?s1 ?s2 ?p1 ?p2 ?o1 ?o2
           draftset-uri
           deleted
           live-graph-uris
           draft-graph-uris]
    :or {?s1 '?s ?s2 '?s
         ?p1 '?p ?p2 '?p
         ?o1 '?o ?o2 '?o}
    :as opts}]
  (let [filter (case deleted
                 :ignore (str "FILTER EXISTS { GRAPH ?dg { ?s1 ?p1 ?o1 } }")
                 :rewrite (str "FILTER NOT EXISTS { GRAPH ?dg { ?s1 ?p1 ?o1 } }")
                 nil)
        ds-values (when draftset-uri (str "VALUES ?ds { <" draftset-uri "> }"))
        live-values (->> live-graph-uris
                         (map #(str "<" % ">"))
                         (string/join " ")
                         (format "VALUES ?lg { %s }"))
        draft-values (->> draft-graph-uris
                          (map #(str "<" % ">"))
                          (string/join " ")
                          (format "VALUES ?dg { %s }"))
        ]
    (str "
DELETE { GRAPH ?g { " ?s1 " " ?p1 " " ?o1 " } }
INSERT { GRAPH ?g { " ?s2 " " ?p2 " " ?o2 " } }
WHERE  {
  GRAPH ?g { " ?s1 " " ?p1 " " ?o1 " }
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
        <http://publishmydata.com/def/drafter/DraftSet> .
    ?g <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
    " filter "
    " ds-values "
    " (when (seq live-graph-uris) live-values) "
    " (when (seq draft-graph-uris) draft-values) "
  }
} ;")))

(defn- rewrite-draftset-q [opts]
  (str
   (rewrite-1-q (assoc opts :?s1 '?lg :?s2 '?dg :deleted :ignore)) \newline
   (rewrite-1-q (assoc opts :?p1 '?lg :?p2 '?dg :deleted :ignore)) \newline
   (rewrite-1-q (assoc opts :?o1 '?lg :?o2 '?dg :deleted :ignore)) \newline))

(defn- unrewrite-draftset-q [opts]
  (str
   (rewrite-1-q (assoc opts :?s1 '?dg :?s2 '?lg)) \newline
   (rewrite-1-q (assoc opts :?p1 '?dg :?p2 '?lg)) \newline
   (rewrite-1-q (assoc opts :?o1 '?dg :?o2 '?lg)) \newline))

(defn rewrite-draftset! [conn opts]
  (->> (rewrite-draftset-q opts)
       (sparql/update! conn)))

(defn unrewrite-draftset! [conn opts]
  (->> (unrewrite-draftset-q opts)
       (sparql/update! conn)))

(defn migrate-graphs-to-live! [repo graphs clock-fn]
  "Migrates a collection of draft graphs to live through a single
  compound SPARQL update statement. Explicit UPDATE statements do not
  take part in transactions on the remote sesame SPARQL client."
  (log/info "Starting make-live for graphs " graphs)
  (when (seq graphs)
    (let [transaction-started-at (clock-fn)
          graph-migrate-queries (mapcat #(:queries (migrate-live-queries repo % transaction-started-at))
                                        graphs)
          fixup-q (unrewrite-draftset-q {:draft-graph-uris graphs})
          update-str (str fixup-q (util/make-compound-sparql-query graph-migrate-queries))]
      (update! repo update-str)))
  (log/info "Make-live for graph(s) " graphs " done"))

(defn calculate-graph-restriction [public-live-graphs live-graph-drafts supplied-draft-graphs]
  (set/union
   (set/difference (set public-live-graphs) (set live-graph-drafts))
   (set supplied-draft-graphs)))

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
        (sparql/add conn graph-uri triple-batch)))))
