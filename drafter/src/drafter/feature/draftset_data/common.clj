(ns drafter.feature.draftset-data.common
  (:require
   [clojure.string :as str]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.draftset :as ds]
   [drafter.rdf.drafter-ontology :refer :all]
   [drafter.rdf.sparql :as sparql]
   [drafter.util :as util]
   [drafter.write-scheduler :as writes]
   [grafter-2.rdf.protocols :as rdf :refer [context map->Triple]]
   [grafter-2.rdf4j.io :refer [rdf-writer]]
   [grafter.vocabularies.dcterms :refer [dcterms:modified]])
  (:import
   java.io.StringWriter
   java.net.URI))

(defn touch-graph-in-draftset
  "Builds and returns an update string to update the dcterms:modified and
   drafter:version of the supplied resource draft-graph/draftset."
  [draftset-ref draft-graph-uri modified-at]
  (let [version (util/version)]
    (str/join
     " ; "
     [(mgmt/set-timestamp draft-graph-uri dcterms:modified modified-at)
      (mgmt/set-timestamp (ds/->draftset-uri draftset-ref) dcterms:modified modified-at)
      (mgmt/set-version (ds/->draftset-uri draftset-ref) version)])))

(defn touch-graph-in-draftset!
  "Updates the dcterms:modified and drafter:version on the given draftgraph and
   draftset."
  [backend draftset-ref draft-graph-uri modified-at]
  (sparql/update! backend
                  (touch-graph-in-draftset draftset-ref draft-graph-uri modified-at)))

(defn quad-batch->graph-triples
  "Extracts the graph-uri from a sequence of quads and converts all
  quads into triples. Batch must be non-empty and each contained quad
  should have the same graph. If the quads have a nil context an
  exception is thrown as drafts for the default graph are not
  currently supported."
  [quads]
  {:pre [(not (empty? quads))]}
  (let [graph-uri (context (first quads))]
    (if (some? graph-uri)
      {:graph-uri graph-uri :triples (map map->Triple quads)}
      (let [sw (StringWriter.)
            msg (format "All statements must have an explicit target graph")]
        (rdf/add (rdf-writer sw :format :nq) (take 5 quads))
        (throw (IllegalArgumentException.
                (str "All statements must have an explicit target graph. The following statements have no graph:\n" sw)))))))

(defn lock-writes-and-copy-graph
  "Calls mgmt/copy-graph to copy a live graph into the draftset, but
  does so with the writes lock engaged.  This allows us to fail
  concurrent sync-writes fast."
  [resources live-graph-uri draft-graph-uri opts]
  (writes/with-lock (:global-writes-lock resources) :copy-graph
    ;; Execute the graph copy inside the write-lock so we can
    ;; fail :blocking-write operations if they are waiting longer than
    ;; their timeout period for us to release it.  These writes would
    ;; likely be blocked inside the database anyway, so this way we
    ;; can fail them fast when they are run behind a long running op.
    (mgmt/copy-graph (:backend resources) live-graph-uri draft-graph-uri opts)))
