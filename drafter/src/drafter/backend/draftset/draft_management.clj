(ns drafter.backend.draftset.draft-management
  (:require
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [drafter.rdf.drafter-ontology :refer :all]
   [drafter.rdf.sparql :as sparql :refer [update!]]
   [drafter.time :as time]
   [drafter.util :as util]
   [grafter-2.rdf4j.io :as rio]
   [grafter-2.rdf4j.repository :as repo]
   [grafter-2.rdf4j.templater :refer [add-properties graph]]
   [grafter.url :as url]
   [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:modified]]
   [grafter.vocabularies.rdf :refer :all])
  (:import java.net.URI
           [java.util UUID]))

(def drafter-state-graph (URI. "http://publishmydata.com/graphs/drafter/drafts"))

(def staging-base (URI. "http://publishmydata.com/graphs/drafter/draft"))

(def to-quads (partial graph drafter-state-graph))

(defn make-draft-graph-uri []
  (url/->java-uri (url/append-path-segments staging-base (str (UUID/randomUUID)))))

(defn with-state-graph
  "Wraps the string in a SPARQL
   GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
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

(defn- has-more-than-one-draft?
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

(defn xsd-datetime
  "Coerce a date into the xsd-datetime string"
  [offset-datetime]
  (str (rio/->backend-type offset-datetime)))

;; TODO surely both of these can be written in terms of upsert-single-object!
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

(defn set-version
  "Returns an update string to update the given subject/resource with
   the supplied version."
  [subject version]
  (str
   "WITH <http://publishmydata.com/graphs/drafter/drafts>"
   "DELETE {"
   "   <" subject "> <" drafter:version "> ?lastvalue ."
   "}"
   "INSERT { "
   "   <" subject "> <" drafter:version "> <" version "> ."
   "}"
   "WHERE {"
   "   <" subject "> ?p ?o . "
   "  OPTIONAL {"
   "     <" subject "> <" drafter:version "> ?lastvalue ."
   "  }"
   "}"))

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

(defn- delete-draft-state-query [draft-graph-uri]
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

(defn- delete-live-graph-from-state-query [live-graph-uri]
  (str
   "DELETE WHERE {"
   (with-state-graph
     "<" live-graph-uri "> a <" drafter:ManagedGraph "> ;"
     "                     ?p ?o")
   "}"))

(defn- copy-graph-query
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

(defn- rewrite-q
  [{:keys [?from ?to draftset-uri deleted live-graph-uris draft-graph-uris]}]
  (let [filter (case deleted
                 :ignore (str "FILTER EXISTS { GRAPH ?dg { ?s_ ?p_ ?o_ } }")
                 :rewrite (str "FILTER NOT EXISTS { GRAPH ?dg { ?s_ ?p_ ?o_ } }")
                 nil)
        ds-values (when draftset-uri (str "VALUES ?ds { <" draftset-uri "> }"))
        live-values (some->> (seq live-graph-uris)
                             (map #(str "<" % ">"))
                             (string/join " ")
                             (format "VALUES ?lg { %s }"))
        draft-values (some->> (seq draft-graph-uris)
                              (map #(str "<" % ">"))
                              (string/join " ")
                              (format "VALUES ?dg { %s }"))
        suffix (->> [filter ds-values live-values draft-values]
                    (remove nil?)
                    (string/join "\n    ")
                    (str "\n    "))]
    (format "
DELETE { GRAPH ?g { %1$s ?p1 ?o1 . ?s2 %1$s ?o2 . ?s3 ?p3 %1$s . } }
INSERT { GRAPH ?g { %2$s ?p1 ?o1 . ?s2 %2$s ?o2 . ?s3 ?p3 %2$s . } }
WHERE {
  GRAPH ?g { { %1$s ?p1 ?o1 } UNION
             { ?s2 %1$s ?o2 } UNION
             { ?s3 ?p3 %1$s } }
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/drafter/DraftSet> .
    ?g <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .%3$s
  }
} ;" ?from ?to suffix)))

(defn rewrite-draftset-q [opts]
  (rewrite-q (assoc opts :?from '?lg :?to '?dg :deleted :ignore)))

(defn- unrewrite-draftset-q [opts]
  (rewrite-q (assoc opts :?from '?dg :?to '?lg)))

(defn rewrite-draftset! [conn opts]
  (->> (rewrite-draftset-q opts)
       (sparql/update! conn)))

(defn unrewrite-draftset! [conn opts]
  (->> (unrewrite-draftset-q opts)
       (sparql/update! conn)))

(defn migrate-graphs-to-live! [repo graphs clock]
  "Migrates a collection of draft graphs to live through a single
  compound SPARQL update statement. Explicit UPDATE statements do not
  take part in transactions on the remote sesame SPARQL client."
  (log/info "Starting make-live for graphs " graphs)
  (when (seq graphs)
    (let [transaction-started-at (time/now clock)
          graph-migrate-queries (mapcat #(:queries (migrate-live-queries repo % transaction-started-at))
                                        graphs)
          fixup-q (unrewrite-draftset-q {:draft-graph-uris graphs})
          update-str (str fixup-q (util/make-compound-sparql-query graph-migrate-queries))]
      (update! repo update-str)))
  (log/info "Make-live for graph(s) " graphs " done"))

(defn calculate-draft-raw-graphs
  "Returns the set of draft data graphs given the set of all visible live graphs,
  the set of graphs which have a draft graph in the draftset and the corresponding
  set of draft graphs."
  [public-live-graphs live-graph-drafts draft-graphs]
  (set/union
    (set/difference (set public-live-graphs) (set live-graph-drafts))
    (set draft-graphs)))

(defn draft-raw-graphs
  "Returns a set of all the raw graphs within a draftset i.e. all the draft graphs and all the
   visible live graphs. If union-with-live? is false there are no visible live graphs, otherwise
   all the live graphs without a corresponding draft graph in the draftset are included."
  [db graph-mapping union-with-live?]
  (let [live-graphs (if union-with-live? (live-graphs db) #{})]
    (calculate-draft-raw-graphs live-graphs (keys graph-mapping) (vals graph-mapping))))

(defn append-data-batch!
  "Appends a sequence of triples to the given draft graph."
  [repo graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (if-not (empty? triple-batch)
    ;;WARNING: This assumes the backend is a sesame backend which is
    ;;true for all current backends.
    (with-open [conn (repo/->connection repo)]
      (repo/with-transaction conn
        (sparql/add conn graph-uri triple-batch)))))
