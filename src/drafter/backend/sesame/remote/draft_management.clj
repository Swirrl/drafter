(ns drafter.backend.sesame.remote.draft-management
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]
            [grafter.rdf.protocols :refer [update!]]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [drafter-state-graph] :as mgmt]
            [drafter.backend.sesame.common.protocols :refer [->sesame-repo ->repo-connection]]))

(defn append-data-batch [backend graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (if-not (empty? triple-batch)
    (with-open [conn (->repo-connection backend)]
      (repo/with-transaction conn
        (add conn graph-uri triple-batch)))))

(defn- sparql-uri-list [uris]
  (string/join " " (map #(str "<" % ">") uris)))

(defn ->sparql-values-binding [e]
  (if (coll? e)
    (str "(" (string/join " " e) ")")
    e))

(defn- meta-pair->values-binding [[uri value]]
  [(str "<" uri ">") (str \" value \")])

(defn meta-pairs->values-bindings [meta-pairs]
  (let [uri-pairs (map meta-pair->values-binding meta-pairs)]
    (string/join " " (map ->sparql-values-binding uri-pairs))))

(defn- append-metadata-to-graphs-query [graph-uris meta-pairs]
  (str "WITH <" drafter-state-graph ">
        DELETE { ?g ?p ?existing }
        INSERT { ?g ?p ?o }
        WHERE {
          VALUES ?g { " (sparql-uri-list graph-uris) " }
          VALUES (?p ?o) { " (meta-pairs->values-bindings meta-pairs)  " }
          OPTIONAL { ?g ?p ?existing }
        }"))

(defn append-metadata-to-graphs! [backend graph-uris meta-pairs]
  (let [update-query (append-metadata-to-graphs-query graph-uris meta-pairs)]
    (update! backend update-query)))

(defn- move-like-tbl-wants-super-slow-on-stardog-though
  "Move's how TBL intended.  Issues a SPARQL MOVE query.
  Note this is super slow on stardog 3.1."
  [source destination]
  ;; Move's how TBL intended...
  (str "MOVE SILENT <" source "> TO <" destination ">"))

(defn- delete-insert-move
  "Move source graph to destination.  Semantically the same as MOVE but done
  with DELETE/INSERT's."
  [source destination]
  ;; TODO: replace this with the SPARQL MOVE query from above it used to be
  ;; faster on stardog to do it this way (and on sesame it didn't make much
  ;; difference)
  ;;
  ;; but stardog claim to have fixed this performance regression in version 3.1.
  ;; so MOVE's may now be faster this way.
  ;;
  ;; See here for details: http://tinyurl.com/jgcytss
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
  (repo/query repo (graph-non-empty-query graph-uri)))

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
   (not (mgmt/has-more-than-one-draft? repo live-graph-uri))))

;;Repository -> String -> { queries: [String], live-graph-uri: String }
(defn- migrate-live-queries [db draft-graph-uri]
  (if-let [live-graph-uri (mgmt/lookup-live-graph db draft-graph-uri)]
    (let [move-query (delete-insert-move draft-graph-uri live-graph-uri)
          delete-state-query (mgmt/delete-draft-state-query draft-graph-uri)
          live-public-query (mgmt/set-isPublic-query live-graph-uri true)
          queries [move-query delete-state-query live-public-query]
          queries (if (should-delete-live-graph-from-state-after-draft-migrate? db draft-graph-uri live-graph-uri)
                    (conj queries (mgmt/delete-live-graph-from-state-query live-graph-uri))
                    queries)]
      {:queries queries
       :live-graph-uri live-graph-uri})))

(defn migrate-graphs-to-live! [backend graphs]
  "Migrates a collection of draft graphs to live through a single
  compound SPARQL update statement. This overrides the default sesame
  implementation which uses a transaction to coordinate the
  updates. Explicit UPDATE statements do not take part in transactions
  on the remote sesame SPARQL client."
  (log/info "Starting make-live for graphs " graphs)
  (let [repo (->sesame-repo backend)
        graph-migrate-queries (mapcat #(:queries (migrate-live-queries repo %)) graphs)
        update-str (util/make-compound-sparql-query graph-migrate-queries)]
    (update! repo update-str))
  (log/info "Make-live for graph(s) " graphs " done"))
