(ns ^:rest-api drafter.feature.draftset-data.show-test
  (:require [clojure.test :as t :refer :all]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [grafter-2.rdf.protocols :refer [context map->Triple]]
            [grafter-2.rdf4j.io :refer [statements]]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(def keys-for-test [[:drafter/routes :draftset/api] :drafter/write-scheduler])

(tc/deftest-system-with-keys get-draftset-data-for-missing-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [response (handler (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :get :headers {"accept" "application/n-quads"}}))]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys get-draftset-data-for-unowned-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        get-data-request (help/get-draftset-quads-request draftset-location test-publisher :nq "false")
        response (handler get-data-request)]
    (tc/assert-is-forbidden-response response)))

(tc/deftest-system-with-keys get-draftset-graph-triples-data
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        input-quads (statements draftset-data-file)]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location input-quads)

    (doseq [[graph quads] (group-by context input-quads)]
      (let [graph-triples (set (help/eval-statements (map map->Triple quads)))
            response-triples (set (help/get-draftset-graph-triples-through-api handler draftset-location test-editor graph "false"))]
        (is (= graph-triples response-triples))))))

(tc/deftest-system-with-keys get-draftset-quads-data
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-data-file "test/resources/test-draftset.trig"]
    (help/append-data-to-draftset-through-api handler test-editor draftset-location draftset-data-file)

    (let [response-quads (set (help/get-user-draftset-quads-through-api handler draftset-location test-editor))
          input-quads (set (help/eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

(tc/deftest-system-with-keys get-draftset-quads-data-with-invalid-accept
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/append-data-to-draftset-through-api handler test-editor draftset-location "test/resources/test-draftset.trig")
    (let [data-request (help/get-draftset-quads-accept-request draftset-location test-editor "text/invalidrdfformat" "false")
          data-response (handler data-request)]
      (tc/assert-is-not-acceptable-response data-response))))

(tc/deftest-system-with-keys get-draftset-quads-data-with-multiple-accepted
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/append-data-to-draftset-through-api handler test-editor draftset-location "test/resources/test-draftset.trig")
    (let [accepted "application/n-quads,application/trig,apllication/trix,application/n-triples,application/rdf+xml,text/turtle"
          data-request (help/get-draftset-quads-accept-request draftset-location test-editor accepted "false")
          data-response (handler data-request)]
      (tc/assert-is-ok-response data-response))))

(tc/deftest-system-with-keys get-draftset-quads-unioned-with-live
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (first (keys grouped-quads))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location graph-to-delete)

    (let [response-quads (set (help/get-user-draftset-quads-through-api handler draftset-location test-editor "true"))
          expected-quads (set (help/eval-statements (mapcat second (rest grouped-quads))))]
      (is (= expected-quads response-quads)))))

(tc/deftest-system-with-keys get-added-draftset-quads-unioned-with-live
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph live-quads] (first grouped-quads)
        [draftset-graph draftset-quads] (second grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler live-quads)
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location draftset-quads)

    (let [response-quads (set (help/get-user-draftset-quads-through-api handler draftset-location test-editor "true"))
          expected-quads (set (help/eval-statements (concat live-quads draftset-quads)))]
      (is (= expected-quads response-quads)))))

(tc/deftest-system-with-keys get-draftset-triples-for-deleted-graph-unioned-with-live
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (ffirst grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location graph-to-delete)

    (let [draftset-triples (help/get-draftset-graph-triples-through-api handler draftset-location test-editor graph-to-delete "true")]
      (is (empty? draftset-triples)))))

(tc/deftest-system-with-keys get-draftset-triples-for-published-graph-not-in-draftset-unioned-with-live
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        [graph graph-quads] (first (group-by context quads))
        draftset-location (help/create-draftset-through-api handler)]
    (help/publish-quads-through-api handler graph-quads)

    (let [draftset-graph-triples (help/get-draftset-graph-triples-through-api handler draftset-location test-editor graph "true")
          expected-triples (help/eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(tc/deftest-system-with-keys get-draftset-graph-triples-request-without-graph
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [data-request {:uri (str draftset-location "/data")
                        :request-method :get
                        :headers {"accept" "application/n-triples"}}
          data-request (tc/with-identity test-editor data-request)
          data-response (handler data-request)]
      (tc/assert-is-not-acceptable-response data-response))))

(tc/deftest-system-with-keys get-draftset-quads-data-with-accept-in-query
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset (help/create-draftset-through-api handler test-editor)]
    (help/append-data-to-draftset-through-api
     handler test-editor draftset "test/resources/test-draftset.trig")
    (let [accepted "*/*"
          data-request (help/get-draftset-quads-accept-request
                        draftset test-editor accepted "false")
          data-request (assoc-in data-request [:params :accept] "text/csv")
          data-response (handler data-request)]
      (tc/assert-is-ok-response data-response)
      (is (= "text/csv" (get-in data-response [:headers "Content-Type"]))))))
