(ns drafter.backend.live
  (:require [clojure.spec.alpha :as s]
            [drafter.backend :as backend]
            [drafter.backend.common :as bprot :refer [->sesame-repo]]
            [drafter.rdf.draft-management :as mgmt]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.repository :as repo]
            [integrant.core :as ig]))

(defrecord RestrictedExecutor [inner restriction]
  bprot/SparqlExecutor

  (prepare-query [this query-string]
    (let [pquery (bprot/prep-and-validate-query inner query-string)]
      (bprot/apply-restriction pquery restriction)))

  bprot/ToRepository
  (->sesame-repo [_] (->sesame-repo inner)))

(extend RestrictedExecutor
  proto/ITripleReadable bprot/itriple-readable-delegate
  proto/ITripleWriteable bprot/itriple-writeable-delegate
  proto/ISPARQLable bprot/isparqlable-delegate
  proto/ISPARQLUpdateable bprot/isparql-updateable-delegate
  repo/ToConnection bprot/to-connection-delegate)

(defn live-endpoint-with-stasher
  "Creates a backend restricted to the live graphs."
  [{:keys [uncached-repo stasher-repo]}]
  ;; TODO: remove need for uncached repo. Doing so will require
  ;; state-graph inception, i.e. storing data on the state graph in
  ;; the stategraph.
  (->RestrictedExecutor stasher-repo (partial mgmt/live-graphs uncached-repo)))

(defmethod ig/pre-init-spec ::endpoint [_]
  (s/keys :req-un [::backend/uncached-repo ::backend/stasher-repo]))

(defmethod ig/init-key ::endpoint [_ opts]
  (live-endpoint-with-stasher opts))
