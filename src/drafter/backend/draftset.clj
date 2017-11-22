(ns drafter.backend.draftset
  (:require [drafter.backend.common
             :as
             bprot
             :refer
             [->sesame-repo apply-restriction]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draftset-management.operations :as dsmgmt]
            [drafter.rdf.rewriting.arq :as arq]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-query-results]]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.repository :as repo]
            [schema.core :as s])
  (:import org.eclipse.rdf4j.model.URI))

(s/defrecord RewritingSesameSparqlExecutor [inner :- (s/protocol bprot/SparqlExecutor)
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

(defn draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [{:keys [backend draftset-ref union-with-live?]}]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)]
    (->RewritingSesameSparqlExecutor backend graph-mapping union-with-live?)))
