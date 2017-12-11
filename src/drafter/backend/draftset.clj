(ns drafter.backend.draftset
  (:require [clojure.spec.alpha :as sp]
            [drafter.backend.spec :as bs]
            [drafter.backend.common :as bprot :refer [->sesame-repo]]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsmgmt]
            [drafter.backend.draftset.rewrite-query :refer [rewrite-sparql-string]]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-query-results]]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [schema.core :as sc])
  (:import org.eclipse.rdf4j.model.URI))

(sc/defrecord RewritingSesameSparqlExecutor [inner :- (sc/protocol bprot/SparqlExecutor)
                                             live->draft :- {URI URI}
                                             union-with-live? :- Boolean]
  bprot/SparqlExecutor
  (prepare-query [this sparql-string]
    (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
          graph-restriction (mgmt/graph-mapping->graph-restriction inner live->draft union-with-live?)
          pquery (bprot/prep-and-validate-query inner rewritten-query-string)
          pquery (bprot/apply-restriction pquery graph-restriction)]
      (rewrite-query-results pquery live->draft)))

  bprot/ToRepository
  (->sesame-repo [_] (->sesame-repo inner)))

(extend RewritingSesameSparqlExecutor
  proto/ITripleReadable bprot/itriple-readable-delegate
  proto/ITripleWriteable bprot/itriple-writeable-delegate
  proto/ISPARQLable bprot/isparqlable-delegate
  proto/ISPARQLUpdateable bprot/isparql-updateable-delegate
  repo/ToConnection bprot/to-connection-delegate)

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
    (->RewritingSesameSparqlExecutor stasher-repo graph-mapping union-with-live?)))

(defmethod ig/pre-init-spec ::endpoint [_]
  (sp/keys :req-un [::bs/uncached-repo ::bs/stasher-repo]))

(defmethod ig/init-key ::endpoint [_ opts]
  opts)
