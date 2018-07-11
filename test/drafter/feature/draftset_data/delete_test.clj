(ns drafter.feature.draftset-data.delete-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.delete :as sut]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(tc/deftest-system delete-draftset-data-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [initial-time (constantly #inst "2017")
        update-time (constantly #inst "2018")
        delete-time (constantly #inst "2019")
        ds (dsops/create-draftset! backend test-editor)]
    

    (th/apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES delete-time))
    (let [ts-3 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
      (t/is (= (.getTime (delete-time))
               (.getTime ts-3))
            "Modified time is updated after delete"))))
