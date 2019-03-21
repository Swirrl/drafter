(ns drafter.feature.draftset-data.append-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.append :as sut]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat
           java.time.OffsetDateTime))

(tc/deftest-system append-data-to-draftset-job-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [initial-time (constantly (OffsetDateTime/parse "2017-01-01T01:01:01Z"))
        update-time  (constantly (OffsetDateTime/parse "2018-01-01T01:01:01Z") )
        delete-time  (constantly (OffsetDateTime/parse "2019-01-01T01:01:01Z"))
        ds (dsops/create-draftset! backend test-editor)]
    (th/apply-job! (sut/append-triples-to-draftset-job backend ds (io/file "./test/test-triple.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph") initial-time))
    (let [ts-1 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
      (t/is (= (.toEpochSecond (initial-time))
               (.toEpochSecond ts-1)))
      (th/apply-job! (sut/append-triples-to-draftset-job backend ds (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph") update-time))
      (let [ts-2 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
        (t/is (= (.toEpochSecond (update-time))
                 (.toEpochSecond ts-2))
              "Modified time is updated after append")

        #_(apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES delete-time))
        #_(let [ts-3 (ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
          (t/is (= (.toEpochSecond (delete-time))
                   (.toEpochSecond ts-3))
                "Modified time is updated after delete"))))))
