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
            [drafter.user-test :refer [test-editor test-publisher]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.async.jobs :as async]
            [grafter-2.rdf4j.formats :as formats]
            [drafter.fixture-data :as fd]
            [drafter.rdf.drafter-ontology :refer [drafter:endpoints]]
            [grafter.vocabularies.dcterms :refer [dcterms:modified]])
  (:import java.net.URI
           java.time.OffsetDateTime
           org.eclipse.rdf4j.rio.RDFFormat))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "test-system.edn")

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

(def keys-for-test [[:drafter/routes :draftset/api] :drafter/write-scheduler :drafter.fixture-data/loader])

(tc/deftest-system-with-keys append-quad-data-with-valid-content-type-to-draftset
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        data-file-path "test/resources/test-draftset.trig"
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

(tc/deftest-system-with-keys append-gzipped-quad-data-to-draftset
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        data-file-path "test/resources/test-draftset.trig"
        data-file (io/file data-file-path)
        draftset-location (help/create-draftset-through-api handler test-editor)
        ss (tc/->gzip-input-stream data-file)
        content-type (.getDefaultMIMEType (formats/->rdf-format data-file))
        append-request (help/append-to-draftset-request test-editor
                                                        draftset-location
                                                        ss
                                                        {:content-type content-type})
        append-request (assoc-in append-request [:headers "content-encoding"] "gzip")
        append-response (handler append-request)]
    (tc/await-success (get-in append-response [:body :finished-job]))

    (let [ds-quads (help/get-draftset-quads-through-api handler draftset-location test-editor)]
      (is (= (set (help/eval-statements (statements data-file))) (set ds-quads))))))

(tc/deftest-system-with-keys append-quad-data-with-metadata
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        data-file-path "test/resources/test-draftset.trig"
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
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        quads (statements "test/resources/test-draftset.trig")
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
  keys-for-test
  [system system-config]
  (with-open [fs (io/input-stream "test/test-triple.nt")]
    (let [handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          request (help/append-to-draftset-request test-editor draftset-location fs "application/n-triples")
          response (handler request)]
      (is (help/is-client-error-response? response)))))

(tc/deftest-system-with-keys append-triple-data-with-metadata
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
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
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler [(first graph-quads)])
    (help/append-triples-to-draftset-through-api handler test-editor draftset-location (rest graph-quads) graph)

    (let [draftset-graph-triples (help/get-draftset-graph-triples-through-api handler draftset-location test-editor graph "false")
          expected-triples (help/eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(t/deftest append-quads-to-endpoints-graph-in-draftset
  (tc/with-system
    keys-for-test
    [system system-config]
    (tc/check-endpoint-graph-consistent system
      (let [handler (get system [:drafter/routes :draftset/api])
            draftset-location (help/create-draftset-through-api handler test-publisher)
            data (io/resource "drafter/feature/draftset_data/append_to_public_endpoint_graph.trig")
            response (help/make-append-data-to-draftset-request handler test-publisher draftset-location data)
            result (tc/await-completion (get-in response [:body :finished-job]))]
        (is (= :error (:type result)))

        (help/publish-draftset-through-api handler draftset-location test-publisher)))))

(t/deftest append-triples-to-endpoints-graph-in-draftset
  (tc/with-system
    keys-for-test
    [system system-config]
    (tc/check-endpoint-graph-consistent
      system
      (let [handler (get system [:drafter/routes :draftset/api])
            draftset-location (help/create-draftset-through-api handler test-publisher)
            triples (statements "test/test-triple.nt")
            request (help/statements->append-triples-request test-publisher draftset-location triples {:graph drafter:endpoints})
            response (handler request)
            result (tc/await-completion (get-in response [:body :finished-job]))]
        (is (= :error (:type result)))
        (help/publish-draftset-through-api handler draftset-location test-publisher)))))

(tc/deftest-system-with-keys append-quad-data-without-content-type-to-draftset
  keys-for-test
  [system system-config]
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          request (help/append-to-draftset-request test-editor draftset-location fs "tmp-content-type")
          request (update-in request [:headers] dissoc "content-type")
          response (handler request)]
      (is (help/is-client-error-response? response)))))

(tc/deftest-system-with-keys append-data-to-non-existent-draftset
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        append-response (help/make-append-data-to-draftset-request handler test-publisher "/v1/draftset/missing" "test/resources/test-draftset.trig")]
    (tc/assert-is-not-found-response append-response)))

(tc/deftest-system-with-keys append-quads-by-non-owner
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        quads (statements "test/resources/test-draftset.trig")
        append-request (help/statements->append-request test-publisher draftset-location quads {:format :nq})
        append-response (handler append-request)]
    (tc/assert-is-forbidden-response append-response)))

(tc/deftest-system-with-keys append-graph-triples-by-non-owner
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        append-request (help/statements->append-triples-request test-publisher draftset-location graph-quads {:graph graph})
        append-response (handler append-request)]
    (tc/assert-is-forbidden-response append-response)))
