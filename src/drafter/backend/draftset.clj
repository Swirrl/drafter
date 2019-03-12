(ns drafter.backend.draftset
  (:require [clojure.spec.alpha :as sp]
            [drafter.backend.common :as bprot :refer [->sesame-repo]]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsmgmt]
            [drafter.backend.draftset.rewrite-query :refer [rewrite-sparql-string]]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-query-results]]
            [drafter.backend.spec :as bs]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.io :as rio]
            [grafter.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [schema.core :as sc]
            [clojure.tools.logging :as log]
            [clojure.set :as set])
  (:import java.io.Closeable
           [org.apache.jena.query QueryFactory Syntax]
           [org.eclipse.rdf4j.model Resource URI]
           org.eclipse.rdf4j.model.impl.URIImpl))

(defn- prepare-rewrite-query
  [conn live->draft sparql-string union-with-live? dataset]
  (let [query (QueryFactory/create sparql-string Syntax/syntaxSPARQL_11)
        query-dataset (bprot/query-dataset-restriction query)
        rewritten-query (rewrite-sparql-string live->draft sparql-string)
        graph-restriction (mgmt/graph-mapping->graph-restriction conn
                                                                 live->draft
                                                                 union-with-live?)]
    (-> conn
        (bprot/prep-and-validate-query rewritten-query)
        (bprot/restrict-query dataset query-dataset graph-restriction)
        (rewrite-query-results live->draft))))

(defn- build-draftset-connection [{:keys [repo live->draft union-with-live?]}]
  (let [conn (repo/->connection repo)]
    (reify
      
      #_bprot/SparqlExecutor
      #_(prepare-query [this sparql-string]
        (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
              graph-restriction (mgmt/graph-mapping->graph-restriction inner live->draft union-with-live?)
              pquery (bprot/prep-and-validate-query inner rewritten-query-string)
              pquery (bprot/apply-restriction pquery graph-restriction)]
          (rewrite-query-results pquery live->draft)))

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
        (let [f (fn next-item [i]
                  (when (.hasNext i)
                    (let [v (.next i)]
                      (lazy-seq (cons (rio/backend-quad->grafter-quad v) (next-item i))))))]
          (let [iter (.getStatements conn nil nil nil infer (into-array Resource (map #(URIImpl. (str %)) (vals live->draft))))]
            (f iter)))))))


(sc/defrecord RewritingSesameSparqlExecutor [repo :- (sc/protocol bprot/SparqlExecutor)
                                             live->draft :- {URI URI}
                                             union-with-live? :- Boolean]
  #_bprot/SparqlExecutor
  #_(prepare-query [this sparql-string]
    (prepare-rewrite-query live->draft sparql-string inner union-with-live?))

  ;; TODO remove this
  bprot/ToRepository
  (->sesame-repo [_]
    (log/warn "DEPRECATED CALL TO ->sesame-repo.  TODO: remove call")
    (->sesame-repo repo))

  repo/ToConnection
  (repo/->connection [this]
    (build-draftset-connection this)))

;; TODO REMOVE THIS function
#_(defn draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [{:keys [backend draftset-ref union-with-live?]}]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)]
    (->RewritingSesameSparqlExecutor backend graph-mapping union-with-live?)))

(defn build-draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [{:keys [repo]} draftset-ref union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping repo draftset-ref)]
    (->RewritingSesameSparqlExecutor repo graph-mapping union-with-live?)))

(defmethod ig/pre-init-spec ::endpoint [_]
  (sp/keys :req-un [::bs/repo]))

(defmethod ig/init-key ::endpoint [_ opts]
  opts)
