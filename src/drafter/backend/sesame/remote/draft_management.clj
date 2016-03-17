(ns drafter.backend.sesame.remote.draft-management
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]
            [grafter.rdf.protocols :as pr]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [drafter-state-graph update! xsd-datetime] :as mgmt]
            [drafter.backend.sesame.common.protocols :refer [->sesame-repo ->repo-connection]])
  (:import [java.util Date]))

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

(defn- move-graph
  "Move's how TBL intended.  Issues a SPARQL MOVE query.
  Note this is super slow on stardog 3.1."
  [source destination]
  ;; Move's how TBL intended...
  (str "MOVE SILENT <" source "> TO <" destination ">"))

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

(defn- update-live-graph-timestamps-query [draft-graph-uri now-ts]
  (let [issued-at (xsd-datetime now-ts)]
    (str "# First remove the modified timestamp from the live graph

WITH <http://publishmydata.com/graphs/drafter/drafts> DELETE {
  ?live dcterms:modified ?modified .
} WHERE {

  VALUES ?draft { <" draft-graph-uri "> }

  ?live a drafter:ManagedGraph ;
     drafter:hasDraft ?draft ;
     dcterms:modified ?modified .

  ?draft a drafter:DraftGraph .
}

; # Then set the modified timestamp on the live graph to be that of the draft graph
WITH <http://publishmydata.com/graphs/drafter/drafts> INSERT {
  ?live dcterms:modified ?modified .
} WHERE {

  VALUES ?draft { <" draft-graph-uri "> }

  ?live a drafter:ManagedGraph ;
        drafter:hasDraft ?draft .

  ?draft a drafter:DraftGraph ;
         dcterms:modified ?modified .
}


; # And finally set a dcterms:issued timestamp if it doesn't have one already.
WITH <http://publishmydata.com/graphs/drafter/drafts> INSERT {
  ?live dcterms:issued " issued-at " .
} WHERE {
  VALUES ?draft { <" draft-graph-uri "> }

  ?live a drafter:ManagedGraph ;
        drafter:hasDraft ?draft .

  FILTER NOT EXISTS { ?live dcterms:issued ?existing . }
}")))

;;Repository -> String -> { queries: [String], live-graph-uri: String }
(defn- migrate-live-queries [db draft-graph-uri transaction-at]
  (if-let [live-graph-uri (mgmt/lookup-live-graph db draft-graph-uri)]
    (let [move-query (move-graph draft-graph-uri live-graph-uri)
          update-timestamps-query (update-live-graph-timestamps-query draft-graph-uri transaction-at)
          delete-state-query (mgmt/delete-draft-state-query draft-graph-uri)
          live-public-query (mgmt/set-isPublic-query live-graph-uri true)
          queries [update-timestamps-query move-query delete-state-query live-public-query]
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
  (when-not (empty? graphs)
    (let [transaction-started-at (Date.)
          repo (->sesame-repo backend)
          graph-migrate-queries (mapcat #(:queries (migrate-live-queries repo % transaction-started-at)) graphs)
          update-str (util/make-compound-sparql-query graph-migrate-queries)]
      (update! repo update-str)))
  (log/info "Make-live for graph(s) " graphs " done"))
