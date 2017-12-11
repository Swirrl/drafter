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
            [schema.core :as sc])
  (:import java.io.Closeable
           [org.eclipse.rdf4j.model Resource URI]
           org.eclipse.rdf4j.model.impl.URIImpl))

(defn- prepare-rewrite-query [stasher-conn uncached-repo live->draft sparql-string union-with-live?]
  (println "prepare-rewrite-query")
  (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
        graph-restriction (mgmt/graph-mapping->graph-restriction uncached-repo live->draft union-with-live?)
        pquery (bprot/prep-and-validate-query stasher-conn rewritten-query-string)
        pquery (bprot/apply-restriction pquery graph-restriction)]
    (rewrite-query-results pquery live->draft)))

(defn- build-draftset-connection [{:keys [stasher-repo uncached-repo live->draft union-with-live?]}]
  (let [stasher-conn (repo/->connection stasher-repo)]
    (reify
      
      #_bprot/SparqlExecutor
      #_(prepare-query [this sparql-string]
        (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
              graph-restriction (mgmt/graph-mapping->graph-restriction inner live->draft union-with-live?)
              pquery (bprot/prep-and-validate-query inner rewritten-query-string)
              pquery (bprot/apply-restriction pquery graph-restriction)]
          (rewrite-query-results pquery live->draft)))

      repo/IPrepareQuery
      (repo/prepare-query* [this sparql-string _]
        (prepare-rewrite-query stasher-conn uncached-repo live->draft sparql-string union-with-live?))

      proto/ISPARQLable
      (proto/query-dataset [this sparql-string _model]
        (let [pquery (repo/prepare-query* this sparql-string _model)]
          (repo/evaluate pquery)))
      
      Closeable
      (close [this]
        (.close stasher-conn))

      ;; For completeness... a to-statements implementation that
      ;; enforces the graph restriction.
      proto/ITripleReadable
      (pr/to-statements [this {:keys [:grafter.repository/infer] :or {infer true}}]
        (let [f (fn next-item [i]
                  (when (.hasNext i)
                    (let [v (.next i)]
                      (lazy-seq (cons (rio/backend-quad->grafter-quad v) (next-item i))))))]
          (let [iter (.getStatements stasher-conn nil nil nil infer (into-array Resource (map #(URIImpl. (str %)) (vals live->draft))))]
            (f iter)))))))


(sc/defrecord RewritingSesameSparqlExecutor [stasher-repo :- (sc/protocol bprot/SparqlExecutor)
                                             uncached-repo :- org.eclipse.rdf4j.repository.Repository
                                             live->draft :- {URI URI}
                                             union-with-live? :- Boolean]
  #_bprot/SparqlExecutor
  #_(prepare-query [this sparql-string]
    (prepare-rewrite-query live->draft sparql-string inner union-with-live?))

  bprot/ToRepository
  (->sesame-repo [_] (->sesame-repo stasher-repo))

  repo/ToConnection
  (repo/->connection [this]
    (build-draftset-connection this)))

;; TODO REMOVE THIS function
(defn draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [{:keys [backend draftset-ref union-with-live?]}]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)]
    (->RewritingSesameSparqlExecutor backend graph-mapping union-with-live?)))

(defn build-draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [{:keys [uncached-repo stasher-repo]} draftset-ref union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping uncached-repo draftset-ref)]
    (->RewritingSesameSparqlExecutor stasher-repo uncached-repo graph-mapping union-with-live?)))

(defmethod ig/pre-init-spec ::endpoint [_]
  (sp/keys :req-un [::bs/uncached-repo ::bs/stasher-repo]))

(defmethod ig/init-key ::endpoint [_ opts]
  opts)
