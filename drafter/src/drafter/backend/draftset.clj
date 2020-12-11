(ns drafter.backend.draftset
  (:require [drafter.backend.common :as bprot :refer [->sesame-repo]]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsmgmt]
            [drafter.backend.draftset.rewrite-query :refer [rewrite-sparql-string]]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-query-results]]
            [grafter-2.rdf.protocols :as proto]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import java.io.Closeable
           [org.apache.jena.query QueryFactory Syntax]
           [org.eclipse.rdf4j.model Resource URI]
           org.eclipse.rdf4j.model.impl.URIImpl
           [org.eclipse.rdf4j.common.iteration Iteration]))

(defn- live->draft-remap-restriction [live->draft restriction]
  (let [dictionary (into {} (map (fn [[k v]] [(str k) v]) live->draft))
        lookup (fn [x] (get dictionary (str x) x))]
    (-> restriction
        (update :default-graph (partial map lookup))
        (update :named-graphs (partial map lookup)))))

(defn- prepare-rewrite-query
  [conn live->draft sparql-string union-with-live? dataset]
  (let [query (QueryFactory/create sparql-string Syntax/syntaxSPARQL_11)
        user-restriction (some->> dataset
                                  (bprot/dataset->restriction)
                                  (live->draft-remap-restriction live->draft))
        query-restriction (->> query
                               (bprot/query-dataset-restriction)
                               (live->draft-remap-restriction live->draft))
        rewritten-query (rewrite-sparql-string live->draft sparql-string)
        graph-restriction (mgmt/graph-mapping->graph-restriction conn
                                                                 live->draft
                                                                 union-with-live?)]
    (-> conn
        (bprot/prep-and-validate-query rewritten-query)
        (bprot/restrict-query user-restriction query-restriction graph-restriction)
        (rewrite-query-results live->draft))))

(defn- build-draftset-connection [{:keys [repo live->draft union-with-live?]}]
  (let [conn (repo/->connection repo)]
    (reify
      repo/IPrepareQuery
      (repo/prepare-query* [this sparql-string dataset]
        (prepare-rewrite-query conn
                               live->draft
                               sparql-string
                               union-with-live?
                               dataset))

      ;; TODO fix this interface to work with pull queries.
      proto/ISPARQLable
      (proto/query-dataset [this sparql-string _model]
        (let [pquery (repo/prepare-query* this sparql-string _model)]
          (repo/evaluate pquery)))

      Closeable
      (close [this]
        (.close conn))

      ;; For completeness... a to-statements implementation that
      ;; enforces the graph restriction.
      proto/ITripleReadable
      (pr/to-statements [this {:keys [:grafter.repository/infer] :or {infer true}}]
        (let [f (fn next-item [^Iteration i]
                  (when (.hasNext i)
                    (let [v (.next i)]
                      (lazy-seq (cons (rio/backend-quad->grafter-quad v) (next-item i))))))]
          (let [iter (.getStatements conn nil nil nil infer (into-array Resource (map #(URIImpl. (str %)) (vals live->draft))))]
            (f iter)))))))

(defrecord RewritingSesameSparqlExecutor [repo live->draft union-with-live?]
  bprot/ToRepository
  (->sesame-repo [_]
    (log/warn "DEPRECATED CALL TO ->sesame-repo.  TODO: remove call")
    (->sesame-repo repo))

  repo/ToConnection
  (repo/->connection [this]
    (build-draftset-connection this)))

(defn build-draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [{:keys [repo]} draftset-ref union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping repo draftset-ref)]
    (->RewritingSesameSparqlExecutor repo graph-mapping union-with-live?)))

(defmethod ig/init-key ::endpoint [_ opts]
  opts)
