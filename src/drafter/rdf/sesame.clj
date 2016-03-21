(ns drafter.rdf.sesame
  (:require [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf :refer [statements]])
  (:import [java.util ArrayList]
           [org.openrdf.rio Rio]
           [org.openrdf.rio.helpers StatementCollector]))

(defn is-quads-format? [rdf-format]
  (.supportsContexts rdf-format))

(defn is-triples-format? [rdf-format]
  (not (is-quads-format? rdf-format)))

(defn read-statements
  "Creates a lazy stream of statements from an input stream containing
  RDF data serialised in the given format."
  ([input rdf-format] (read-statements input rdf-format jobs/batched-write-size))
  ([input rdf-format batch-size]
   (statements input
               :format rdf-format
               :buffer-size batch-size)))
