(ns ^:rest-api drafter.feature.draftset.delete-test
  (:require [clojure.set :as set]
            [clojure.test :as t :refer [is testing]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [grafter-2.rdf.protocols :refer [->Quad ->Triple context map->Triple]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [statements]]
            [drafter.async.jobs :as async]
            [martian.encoders :as enc])
  (:import java.net.URI
           java.time.OffsetDateTime))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(defn- create-delete-draftset-request
  ([draftset-location user]
   (create-delete-draftset-request draftset-location user nil))
  ([draftset-location user metadata]
   (tc/with-identity user
                     (cond-> {:uri draftset-location
                              :request-method :delete}
                             metadata (merge {:params {:metadata (enc/json-encode metadata)}})))))

(def keys-for-test [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler])

(tc/deftest-system-with-keys delete-draftset-test
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        delete-response (handler (create-delete-draftset-request draftset-location test-editor))]
    (tc/assert-is-accepted-response delete-response)
    (tc/await-success (get-in delete-response [:body :finished-job]))

    (let [get-response (handler (tc/with-identity test-editor {:uri draftset-location :request-method :get}))]
      (tc/assert-is-not-found-response get-response))))

(tc/deftest-system-with-keys delete-draftset-with-metadata-test
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        delete-request (create-delete-draftset-request draftset-location test-editor {:title "Deleting draftset"})
        delete-response (handler delete-request)]
    (tc/await-success (:finished-job (:body delete-response)))

    (let [job (-> delete-response :body :finished-job tc/job-path->job-id async/complete-job)]
      (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
      (is (= "Deleting draftset" (-> job :metadata :title))))))

(tc/deftest-system-with-keys delete-non-existent-draftset-test
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api]]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [delete-response (handler (create-delete-draftset-request "/v1/draftset/missing" test-publisher))]
    (tc/assert-is-not-found-response delete-response)))

(tc/deftest-system-with-keys delete-draftset-by-non-owner-test
  [:drafter.fixture-data/loader :drafter/routes :draftset/api]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        delete-response (handler (create-delete-draftset-request draftset-location test-manager))]
    (tc/assert-is-forbidden-response delete-response)))

(tc/deftest-system-with-keys delete-non-existent-live-graph-in-draftset
  [:drafter.fixture-data/loader :drafter/routes :draftset/api]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        graph-to-delete "http://live-graph"
        delete-request (help/delete-draftset-graph-request test-editor draftset-location "http://live-graph")]

    (testing "silent"
      (let [delete-request (assoc-in delete-request [:params :silent] "true")
            delete-response (handler delete-request)]
        (tc/assert-is-ok-response delete-response)))

    (testing "malformed silent flag"
      (let [delete-request (assoc-in delete-request [:params :silent] "invalid")
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))

    (testing "not silent"
      (let [delete-request (help/delete-draftset-graph-request test-editor draftset-location "http://live-graph")
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))))

(tc/deftest-system-with-keys delete-non-existent-live-graph-in-draftset-async
  [:drafter.fixture-data/loader :drafter/routes :draftset/api]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        graph-to-delete "http://live-graph"
        delete-request (-> (help/delete-draftset-graph-request
                             test-editor draftset-location
                             "http://live-graph")
                         (assoc-in [:headers "perform-async"] "true"))]

    (testing "silent"
      (let [delete-request (assoc-in delete-request [:params :silent] "true")
            {:keys [status body]} (handler delete-request)]
      (is (= 202 status))
      (is (contains? body :finished-job))
      (is (= :ok (:type body)))))

    (testing "malformed silent flag"
      (let [delete-request (assoc-in delete-request [:params :silent] "invalid")
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))

    (testing "not silent"
      (let [delete-request (help/delete-draftset-graph-request test-editor draftset-location "http://live-graph")
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))))

(tc/deftest-system-with-keys delete-graph-by-non-owner
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]

  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        [graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)

    (let [delete-request (help/delete-draftset-graph-request test-publisher draftset-location graph)
          delete-response (handler delete-request)]
      (tc/assert-is-forbidden-response delete-response))))

(tc/deftest-system-with-keys modifying-in-draftset-updates-modified-timestamp-test
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler :drafter.stasher/repo]
  [{handler [:drafter/routes :draftset/api] repo :drafter.stasher/repo} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (help/create-draftset-through-api handler test-editor)
        get-draft-graph-modified-at
        (fn []
          ;; There is only one draftgraph in this
          ;; test - so we can get away with a bit of
          ;; a sloppy query.
          (-> repo
              (sparql/eager-query
               (str "SELECT ?modified {"
                    "   ?draftgraph a <" drafter:DraftGraph "> ;"
                    "                 <" drafter:modifiedAt ">   ?modified ."
                    "}"))
              (first)
              (:modified)))]

    (testing "Publishing some triples sets the modified time"
      (help/append-triples-to-draftset-through-api handler test-editor draftset-location quads "http://foo/")

      (let [first-timestamp (get-draft-graph-modified-at)]
        (is (instance? OffsetDateTime first-timestamp))

        (testing "Publishing more triples afterwards updates the modified time"

          (help/append-triples-to-draftset-through-api handler test-editor draftset-location quads "http://foo/")
          (let [second-timestamp (get-draft-graph-modified-at)]
            (is (instance? OffsetDateTime second-timestamp))

            (is (.isBefore first-timestamp
                           second-timestamp)
                "Modified time is updated after append")

            (help/delete-triples-through-api handler test-editor draftset-location quads "http://foo/")
            (let [third-timestamp (get-draft-graph-modified-at)]

              (is (.isBefore second-timestamp
                             third-timestamp)
                  "Modified time is updated after delete"))))))))

(tc/deftest-system-with-keys delete-quads-from-live-graphs-in-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        to-delete (map (comp first second) grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)

    (let [{graph-info :changes :as draftset-info} (help/delete-quads-through-api handler test-editor draftset-location to-delete)
          ds-graphs (keys graph-info)
          expected-graphs (map first grouped-quads)
          draftset-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")
          expected-quads (help/eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-graphs) (set ds-graphs)))
      (is (= (set expected-quads) (set draftset-quads))))))

(tc/deftest-system-with-keys delete-quads-from-graph-not-in-live
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        to-delete [(->Quad (URI. "http://s1") (URI. "http://p1") (URI. "http://o1") (URI. "http://missing-graph1"))
                   (->Quad (URI. "http://s2") (URI. "http://p2") (URI. "http://o2") (URI. "http://missing-graph2"))]
        draftset-info (help/delete-quads-through-api handler test-editor draftset-location to-delete)]
    (is (empty? (keys (:changes draftset-info))))))

(tc/deftest-system-with-keys delete-quads-only-in-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context draftset-quads)]

    (help/append-quads-to-draftset-through-api handler test-editor draftset-location draftset-quads)

    (let [
          ;;NOTE: input data should contain at least two statements in each graph!
          ;;delete one quad from each, so all graphs will be non-empty after delete operation
          to-delete (map (comp first second) grouped-quads)
          draftset-info (help/delete-quads-through-api handler test-editor draftset-location to-delete)
          expected-quads (help/eval-statements (mapcat (comp rest second) grouped-quads))
          actual-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")]
      (is (= (set expected-quads) (set actual-quads))))))

(tc/deftest-system-with-keys delete-all-quads-from-draftset-graph
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        initial-statements (statements "test/resources/test-draftset.trig")
        grouped-statements (group-by context initial-statements)
        [graph graph-statements] (first grouped-statements)]
    (help/append-data-to-draftset-through-api handler test-editor draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (help/delete-quads-through-api handler test-editor draftset-location graph-statements)
          expected-graphs (set (map :c initial-statements))
          draftset-graphs (tc/key-set (:changes draftset-info))]
      ;;graph should still be in draftset even if it is empty since it should be deleted on publish
      (is (= expected-graphs draftset-graphs)))))

(tc/deftest-system-with-keys delete-quads-with-malformed-body
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        body (tc/string->input-stream "NOT NQUADS")
        delete-request (help/create-delete-quads-request test-editor
                                                         draftset-location
                                                         body
                                                         {:content-type (.getDefaultMIMEType (formats/->rdf-format :nq))})
        delete-response (handler delete-request)
        job-result (tc/await-completion (get-in delete-response [:body :finished-job]))]
    (is (jobs/failed-job-result? job-result))))

(t/deftest delete-triples-from-graph-in-live
  (tc/with-system
    keys-for-test
    [{handler [:drafter/routes :draftset/api]} system]
    (let [quads (statements "test/resources/test-draftset.trig")
          grouped-quads (group-by context quads)
          [live-graph graph-quads] (first grouped-quads)
          draftset-location (help/create-draftset-through-api handler test-editor)]

      (help/publish-quads-through-api handler quads)
      (let [draftset-info (help/delete-quads-through-api handler test-editor draftset-location [(first graph-quads)])
            draftset-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")
            expected-quads (help/eval-statements (rest graph-quads))]
        (is (= #{live-graph} (tc/key-set (:changes draftset-info))))
        (is (= (set expected-quads) (set draftset-quads)))))))

(tc/deftest-system-with-keys delete-triples-from-graph-not-in-live
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        to-delete [(->Triple (URI. "http://s1") (URI. "http://p1") (URI. "http://o1"))
                   (->Triple (URI. "http://s2") (URI. "http://p2") (URI. "http://o2"))]
        draftset-info (help/delete-draftset-triples-through-api handler test-editor draftset-location to-delete (URI. "http://missing"))
        draftset-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")]

    ;;graph should not exist in draftset since it was not in live
    (is (empty? (:changes draftset-info)))
    (is (empty? draftset-quads))))

(tc/deftest-system-with-keys delete-graph-triples-only-in-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))
        [graph graph-quads] (first (group-by context draftset-quads))
        quads-to-delete (take 2 graph-quads)
        triples-to-delete (map map->Triple quads-to-delete)]

    (help/append-data-to-draftset-through-api handler test-editor draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (help/delete-draftset-triples-through-api handler test-editor draftset-location triples-to-delete graph)
          quads-after-delete (set (help/get-draftset-quads-through-api handler draftset-location test-editor))
          expected-quads (set (help/eval-statements (set/difference draftset-quads quads-to-delete)))]
      (is (= expected-quads quads-after-delete)))))

(tc/deftest-system-with-keys delete-all-triples-from-graph
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler :drafter/global-writes-lock]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        triples-to-delete (map map->Triple graph-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))]

    (help/publish-quads-through-api handler quads)

    (let [draftset-info (help/delete-draftset-triples-through-api handler test-editor draftset-location triples-to-delete graph)
          draftset-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")
          draftset-graphs (tc/key-set (:changes draftset-info))]

      (is (= #{graph} draftset-graphs))
      (is (empty? draftset-quads)))))

(tc/deftest-system-with-keys delete-draftset-triples-request-without-graph-parameter
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-quads (statements "test/resources/test-draftset.trig")]
    (help/append-data-to-draftset-through-api handler test-editor draftset-location "test/resources/test-draftset.trig")

    (with-open [input-stream (help/statements->input-stream (take 2 draftset-quads) :nt)]
      (let [delete-request (help/create-delete-quads-request test-editor
                                                             draftset-location
                                                             input-stream
                                                             {:content-type (.getDefaultMIMEType (formats/->rdf-format :nt))})
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))))

(tc/deftest-system-with-keys delete-triples-with-malformed-body
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        body (tc/string->input-stream "NOT TURTLE")
        delete-request (help/create-delete-quads-request test-editor
                                                         draftset-location
                                                         body
                                                         {:content-type (.getDefaultMIMEType (formats/->rdf-format :ttl))})
        delete-request (assoc-in delete-request [:params :graph] "http://test-graph")
        delete-response (handler delete-request)
        job-result (tc/await-completion (get-in delete-response [:body :finished-job]))]
    (is (jobs/failed-job-result? job-result))))
