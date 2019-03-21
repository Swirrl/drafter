(ns drafter.feature.draftset-data.delete-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.delete :as sut]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat
           java.time.OffsetDateTime))

(tc/deftest-system delete-draftset-data-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [initial-time (constantly (OffsetDateTime/parse "2017-01-01T01:01:01Z"))
        update-time (constantly (OffsetDateTime/parse "2018-01-01T01:01:01Z"))
        delete-time (constantly (OffsetDateTime/parse "2019-01-01T01:01:01Z"))
        ds (dsops/create-draftset! backend test-editor)]


    (th/apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES delete-time))
    (let [ts-3 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
      (t/is (.isEqual (delete-time)
                       ts-3)
            "Modified time is updated after delete"))))
