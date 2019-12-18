(ns ^:rest-api drafter.feature.draftset-data.delete-test
  (:require [clojure.java.io :as io]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [clojure.test :as t :refer [is]]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.append :as append]
            [drafter.feature.draftset-data.delete :as sut]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.rdf.sesame :refer [is-quads-format? read-statements]]
            [ring.mock.request :as req]
            [clojure.string :as string]
            [drafter.async.responses :as r])
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
                                                          ds
                                                          (io/file "./test/test-triple.nt")
                                                          RDFFormat/NTRIPLES
                                                          (URI. "http://foo/graph") initial-time))

    (th/apply-job! (sut/delete-triples-from-draftset-job resources
                                                         dummy
                                                         ds
                                                         (URI. "http://foo/graph")
                                                         (io/file "./test/test-triple-2.nt")
                                                         RDFFormat/NTRIPLES delete-time))
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
          delete-request (help/create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (handler delete-request)]
      (tc/assert-is-unsupported-media-type-response delete-response))))

(defn sync! [handler response]
  (if (= (:status response) 202)
    (let [job-path (-> response :body :finished-job)
         job-id (-> job-path (string/split #"/") last r/try-parse-uuid)]
     (loop []
       (let [r (handler (tc/with-identity test-editor
                          (req/request :get (str "/v1/status/jobs/" job-id))))]
         (if (-> r :body :status (= :pending))
           (do
             (Thread/sleep 500)
             (recur))
           r))))
    (throw (ex-info "Unexpected response"
                    {:type :unexpected-response :response response}))))

(tc/deftest-system-with-keys update-delete-then-add-test
  [:drafter.fixture-data/loader :drafter/write-scheduler
   :drafter.routes/draftsets-api :drafter.routes/jobs-status]
  [{handler :drafter.routes/draftsets-api
    status  :drafter.routes/jobs-status} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        graph "http://test"
        graph-params {:graph graph}
        del-resource (io/resource "update-del-triples.rdf")
        add-resource (io/resource "update-add-triples.rdf")
        n-triples "application/n-triples"
        n-quads "application/n-quads"]
    (with-open [del-triples-1 (io/input-stream del-resource)
                del-triples-2 (io/input-stream del-resource)
                add-triples-1 (io/input-stream add-resource)
                add-triples-2 (io/input-stream add-resource)]
      (let [add-first-request (-> test-editor
                                  (help/append-to-draftset-request draftset-location
                                                                   del-triples-1
                                                                   n-triples)
                                  (assoc :params graph-params))
            add-first-response (handler add-first-request)
            ;; Syncing here to ensure the setup add is fully added
            _ (is (= (-> status (sync! add-first-response) :body :status) :complete))
            delete-request (-> test-editor
                               (help/create-delete-quads-request draftset-location
                                                                 del-triples-2
                                                                 n-triples)
                               (assoc :params graph-params))
            delete-response (handler delete-request)
            ;; Not syncing here to test potential inconsistency - if we do
            ;; actually sync here, test passes
            ;; _ (sync! status delete-response)
            add-request (-> (help/append-to-draftset-request test-editor
                                                             draftset-location
                                                             add-triples-1
                                                             n-triples)
                            (assoc :params graph-params))
            add-response (handler add-request)
            ;; Syncing here to ensure added triples are fully processed
            _ (is (= (-> status (sync! add-response) :body :status) :complete))
            triples (read-statements add-triples-2 n-triples)
            quads (->> triples (map #(assoc % :c (URI. graph))) set)
            ;; Make sure delete has finished before checking what's been stored
            _ (is (= (-> status (sync! delete-response) :body :status) :complete))
            stored-quads (-> test-editor
                             (tc/with-identity {:uri (str draftset-location "/data")
                                                :request-method :get
                                                :headers {"accept" n-quads}
                                                :params {:union-with-live "false"}})
                             (handler)
                             (:body)
                             (read-statements n-quads)
                             (set))]
        (is (= 13 (count stored-quads)))
        (is (= quads stored-quads))))))
