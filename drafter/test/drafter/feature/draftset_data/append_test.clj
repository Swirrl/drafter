(ns ^:rest-api drafter.feature.draftset-data.append-test
  (:require [clojure.java.io :as io]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [clojure.test :as t :refer [is]]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.append :as sut]
            [drafter.feature.draftset-data.test-helper :as th]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.async.jobs :as async])
  (:import java.net.URI
           java.time.OffsetDateTime
           org.eclipse.rdf4j.rio.RDFFormat))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "test-system.edn")

(def dummy "dummy@user.com")

(tc/deftest-system append-data-to-draftset-job-test
  [{:keys [:drafter/backend :drafter/global-writes-lock]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [initial-time (constantly (OffsetDateTime/parse "2017-01-01T01:01:01Z"))
        update-time  (constantly (OffsetDateTime/parse "2018-01-01T01:01:01Z") )
        delete-time  (constantly (OffsetDateTime/parse "2019-01-01T01:01:01Z"))
        ds (dsops/create-draftset! backend test-editor)
        resources {:backend backend :global-writes-lock global-writes-lock}]
    (th/apply-job! (sut/append-data-to-draftset-job (io/file "./test/test-triple.nt")
                                                    resources
                                                    dummy
                                                       {:rdf-format RDFFormat/NTRIPLES
                                                        :graph (URI. "http://foo/graph")
                                                        :draftset-id ds}
                                                       initial-time))
    (let [ts-1 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
      (t/is (= (.toEpochSecond (initial-time))
               (.toEpochSecond ts-1)))
      (th/apply-job! (sut/append-data-to-draftset-job (io/file "./test/test-triple-2.nt")
                                                      resources
                                                      dummy
                                                      {:rdf-format RDFFormat/NTRIPLES
                                                       :graph (URI. "http://foo/graph")
                                                       :draftset-id ds}
                                                      update-time))
      (let [ts-2 (th/ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
        (t/is (= (.toEpochSecond (update-time))
                 (.toEpochSecond ts-2))
              "Modified time is updated after append")

        #_(apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES delete-time))
        #_(let [ts-3 (ensure-draftgraph-and-draftset-modified backend ds "http://foo/graph")]
          (t/is (= (.toEpochSecond (delete-time))
                   (.toEpochSecond ts-3))
                "Modified time is updated after delete"))))))

(tc/deftest-system-with-keys append-quad-data-with-valid-content-type-to-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)
    (let [draftset-graphs (tc/key-set (:changes (help/get-draftset-info-through-api handler draftset-location test-editor)))
          graph-statements (group-by context quads)]
      (doseq [[live-graph graph-quads] graph-statements]
        (let [graph-triples (help/get-draftset-graph-triples-through-api handler draftset-location test-editor live-graph "false")
              expected-statements (map map->Triple graph-quads)]
          (is (contains? draftset-graphs live-graph))
          (is (set expected-statements) (set graph-triples)))))))

(tc/deftest-system-with-keys append-quad-data-with-metadata
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (help/create-draftset-through-api handler test-editor)
        request (help/statements->append-request test-editor
                                                 draftset-location
                                                 quads
                                                 {:metadata {:title "Custom job title"} :format :nq})
        response (handler request)]
    (tc/await-success (get-in response [:body :finished-job]))

    (let [job (-> response :body :finished-job tc/job-path->job-id async/complete-job)]
      (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
      (is (= "Custom job title" (-> job :metadata :title))))))

(tc/deftest-system-with-keys append-quad-data-to-graph-which-exists-in-live
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        live-quads (map (comp first second) grouped-quads)
        quads-to-add (rest (second (first grouped-quads)))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler live-quads)
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads-to-add)

    ;;draftset itself should contain the live quads from the graph
    ;;added to along with the quads explicitly added. It should
    ;;not contain any quads from the other live graph.
    (let [draftset-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")
          expected-quads (help/eval-statements (second (first grouped-quads)))]
      (is (= (set expected-quads) (set draftset-quads))))))

(tc/deftest-system-with-keys append-triple-data-to-draftset-test
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (with-open [fs (io/input-stream "test/test-triple.nt")]
    (let [draftset-location (help/create-draftset-through-api handler test-editor)
          request (help/append-to-draftset-request test-editor draftset-location fs "application/n-triples")
          response (handler request)]
      (is (help/is-client-error-response? response)))))

(tc/deftest-system-with-keys append-triple-data-with-metadata
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        triples (statements "test/test-triple.nt")
        request (help/statements->append-triples-request test-editor
                                                         draftset-location
                                                         triples
                                                         {:metadata {:title "Custom job title"}
                                                          :graph "http://graph-uri"})
        response (handler request)]
    (tc/await-success (get-in response [:body :finished-job]))

    (let [job (-> response :body :finished-job tc/job-path->job-id async/complete-job)]
      (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
      (is (= "Custom job title" (-> job :metadata :title))))))

(tc/deftest-system-with-keys append-triples-to-graph-which-exists-in-live
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler [(first graph-quads)])
    (help/append-triples-to-draftset-through-api handler test-editor draftset-location (rest graph-quads) graph)

    (let [draftset-graph-triples (help/get-draftset-graph-triples-through-api handler draftset-location test-editor graph "false")
          expected-triples (help/eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(tc/deftest-system-with-keys append-quad-data-without-content-type-to-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (help/create-draftset-through-api handler test-editor)
          request (help/append-to-draftset-request test-editor draftset-location fs "tmp-content-type")
          request (update-in request [:headers] dissoc "content-type")
          response (handler request)]
      (is (help/is-client-error-response? response)))))

(tc/deftest-system-with-keys append-data-to-non-existent-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [append-response (help/make-append-data-to-draftset-request handler test-publisher "/v1/draftset/missing" "test/resources/test-draftset.trig")]
    (tc/assert-is-not-found-response append-response)))

(tc/deftest-system-with-keys append-quads-by-non-owner
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        quads (statements "test/resources/test-draftset.trig")
        append-request (help/statements->append-request test-publisher draftset-location quads {:format :nq})
        append-response (handler append-request)]
    (tc/assert-is-forbidden-response append-response)))

(tc/deftest-system-with-keys append-graph-triples-by-non-owner
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        append-request (help/statements->append-triples-request test-publisher draftset-location graph-quads {:graph graph})
        append-response (handler append-request)]
    (tc/assert-is-forbidden-response append-response)))
