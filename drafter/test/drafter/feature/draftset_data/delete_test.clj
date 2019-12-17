(ns ^:rest-api drafter.feature.draftset-data.delete-test
  (:require [clojure.java.io :as io]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.append :as append]
            [drafter.feature.draftset-data.delete :as sut]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.feature.draftset.test-helper :as help]
            [grafter-2.rdf4j.io :as gio]
            [drafter.async.jobs :as async]
            [grafter-2.rdf.protocols :as pr])
  (:import java.net.URI
           java.time.OffsetDateTime
           org.eclipse.rdf4j.rio.RDFFormat))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(def dummy "dummy@user.com")

(tc/deftest-system-with-keys delete-draftset-data-test
  [:drafter/backend :drafter/global-writes-lock :drafter/write-scheduler :drafter.fixture-data/loader]
  [{:keys [:drafter/backend :drafter/global-writes-lock]} system]
  (let [initial-time (constantly (OffsetDateTime/parse "2017-01-01T01:01:01Z"))
        update-time (constantly (OffsetDateTime/parse "2018-01-01T01:01:01Z"))
        delete-time (constantly (OffsetDateTime/parse "2019-01-01T01:01:01Z"))
        ds (dsops/create-draftset! backend test-editor)
        resources {:backend backend :global-writes-lock global-writes-lock}]

    (th/apply-job! (append/append-triples-to-draftset-job resources
                                                          dummy
                                                          (io/file "./test/test-triple.nt")
                                                          {:rdf-format RDFFormat/NTRIPLES
                                                           :graph (URI. "http://foo/graph")
                                                           :draftset-id ds}
                                                          initial-time))

    (th/apply-job! (sut/delete-triples-from-draftset-job resources
                                                         dummy
                                                         (io/file "./test/test-triple-2.nt")
                                                         {:draftset-id ds
                                                          :graph (URI. "http://foo/graph")
                                                          :format RDFFormat/NTRIPLES}
                                                          delete-time))
    (let [ts-3 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
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
          delete-request (help/create-delete-quads-request test-editor
                                                           draftset-location
                                                           input-stream
                                                           {:content-type "application/unknown-quads-format"})
          delete-response (handler delete-request)]
      (tc/assert-is-unsupported-media-type-response delete-response))))

(tc/deftest-system-with-keys delete-draftset-data-with-metadata-test
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads-path "test/resources/test-draftset.trig"
        triples-path "./test/test-triple.nt"
        draftset-location (help/create-draftset-through-api handler test-editor)]

    (t/testing "Deleting quads with metadata"
      (let [quads (gio/statements quads-path)
            delete-request (help/create-delete-statements-request test-editor
                                                                  draftset-location
                                                                  quads
                                                                  {:format :trig
                                                                   :metadata {:title "Custom job title"}})
            delete-response (handler delete-request)]
        (tc/await-success (get-in delete-response [:body :finished-job]))

        (let [job (-> delete-response :body :finished-job tc/job-path->job-id async/complete-job)]
          (t/is (= #{:title :draftset :operation} (-> job :metadata keys set)))
          (t/is (= "Custom job title" (-> job :metadata :title))))))

    (t/testing "Deleting triples with metadata"
      (let [triples (gio/statements triples-path)
            delete-request (help/create-delete-triples-request test-editor
                                                               draftset-location
                                                               triples
                                                               {:graph (URI. "http://graph-uri")
                                                                :metadata {:title "Custom job title"}})
            delete-response (handler delete-request)]
        (tc/await-success (get-in delete-response [:body :finished-job]))

        (let [job (-> delete-response :body :finished-job tc/job-path->job-id async/complete-job)]
          (t/is (= #{:title :draftset :operation} (-> job :metadata keys set)))
          (t/is (= "Custom job title" (-> job :metadata :title))))))))
