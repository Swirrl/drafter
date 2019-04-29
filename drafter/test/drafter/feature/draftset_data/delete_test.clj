(ns drafter.feature.draftset-data.delete-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.delete :as sut]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager]])
  (:import java.net.URI
           java.time.OffsetDateTime
           org.eclipse.rdf4j.rio.RDFFormat))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(tc/deftest-system delete-draftset-data-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [initial-time (constantly (OffsetDateTime/parse "2017-01-01T01:01:01Z"))
        update-time (constantly (OffsetDateTime/parse "2018-01-01T01:01:01Z"))
        delete-time (constantly (OffsetDateTime/parse "2019-01-01T01:01:01Z"))
        ds (dsops/create-draftset! backend test-editor)]

    ;; TODO: the following passes on `master` _ONLY_ when run as part of the
    ;; full suite of tests I.E., `lein test ;; drafter.feature.draftset-data.delete-test` fails
    ;; the same(ish) test in `append-data-to-draftset-job-test` is also commented... WTF!?
    #_(th/apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES delete-time))
    #_(let [ts-3 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
        (t/is (.isEqual (delete-time)
                        ts-3)
              "Modified time is updated after delete"))))

(tc/deftest-system-with-keys delete-draftset-data-for-non-existent-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} system]
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [delete-request (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :delete :body fs})
          delete-response (handler delete-request)]
      (tc/assert-is-not-found-response delete-response))))

(tc/deftest-system-with-keys delete-draftset-data-request-with-unknown-content-type
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} system]
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (help/create-draftset-through-api handler test-editor)
          delete-request (help/create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (handler delete-request)]
      (tc/assert-is-unsupported-media-type-response delete-response))))
