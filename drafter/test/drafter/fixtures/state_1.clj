(ns drafter.fixtures.state-1
  (:require [drafter.rdf.drafter-ontology :refer [draftset-id->uri modified-times-graph-uri]]
            [grafter-2.rdf.protocols :as rdf])
  (:import java.net.URI))

(def expected-live-graphs #{(URI. "http://live-and-ds1-and-ds2")
                            (URI. "http://live-only")
                            modified-times-graph-uri})

(def ds-1 (draftset-id->uri "ds-1"))

(def ds-1-dg-1-data #{(rdf/->Triple (URI. "http://unpublished-graph-ds1")
                                    (URI. "http://unpublished-graph-ds1")
                                    (URI. "http://unpublished-graph-ds1"))})

(def ds-1-subjects #{(URI. "http://unpublished-graph-ds1")
                     (URI. "http://a")
                     (URI. "http://live-only")})

(def ds-2 (draftset-id->uri "ds-2"))
