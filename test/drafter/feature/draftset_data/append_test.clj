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
           org.eclipse.rdf4j.rio.RDFFormat))

(tc/deftest-system append-data-to-draftset-job-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [initial-time (constantly #inst "2017")
        update-time (constantly #inst "2018")
        delete-time (constantly #inst "2019")
        ds (dsops/create-draftset! backend test-editor)]
    (th/apply-job! (sut/append-triples-to-draftset-job backend ds (io/file "./test/test-triple.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph") initial-time))
    (let [ts-1 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
      (t/is (= (.getTime (initial-time))
               (.getTime ts-1)))
      (th/apply-job! (sut/append-triples-to-draftset-job backend ds (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph") update-time))
      (let [ts-2 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
        (t/is (= (.getTime (update-time))
                 (.getTime ts-2))
              "Modified time is updated after append")

        #_(apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES delete-time))
        #_(let [ts-3 (ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
          (t/is (= (.getTime (delete-time))
                   (.getTime ts-3))
                "Modified time is updated after delete"))))))

