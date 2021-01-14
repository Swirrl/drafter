(ns ^:rest-api drafter.feature.draftset-data.common-test
  (:require [clojure.test :as t :refer [is testing]]
            [drafter.feature.draftset-data.common :as sut]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [grafter-2.rdf.protocols :as pr :refer [->Quad triple=]])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(t/deftest quad-batch->graph-triples-test
  (t/testing "Batch quads have nil graph"
    (let [quads [(->Quad (URI. "http://s1") (URI. "http://p1") "o1" nil)
                 (->Quad (URI. "http://s2") (URI. "http://p2") "o2" nil)]]
      (t/is (thrown? IllegalArgumentException (sut/quad-batch->graph-triples quads)))))

  (t/testing "Non-empty batch"
    (let [guri "http://graph"
          quads (map #(->Quad (str "http://s" %) (str "http://p" %) (str "http://o" %) guri) (range 1 10))
          {:keys [graph-uri triples]} (sut/quad-batch->graph-triples quads)]
      (t/is (= guri graph-uri))
      (t/is (every? identity (map triple= quads triples))))))

(def system-config "test-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api] :drafter/write-scheduler :drafter.fixture-data/loader])

(tc/deftest-system-with-keys protected-graphs-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])]
    (testing "Cannot operate on drafter's graphs"
      (let [g (URI. "http://publishmydata.com/graphs/drafter/drafts")
            draftset-location (help/create-draftset-through-api handler test-editor)
            quads [(pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)]]

        (testing "append draftset-data ..."
          (let [request (help/statements->append-request
                         test-editor draftset-location quads {:format :nq})
                response (-> (handler request)
                             (get-in [:body :finished-job])
                             (tc/await-completion))]
            (is (= :error (:type response)))
            (is (= :protected-graph-modification-error
                   (-> response :details :error)))))

        (testing "delete draftset-data ..."
          (let [request (help/create-delete-statements-request
                          test-editor draftset-location quads {:format :nq})
                response (-> (handler request)
                             (get-in [:body :finished-job])
                             (tc/await-completion))]
            (is (empty? (-> response :details :changes)))))

        (testing "delete by-graph ..."
          (let [request (help/delete-draftset-graph-request
                          test-editor draftset-location g)
                response (handler request)]
            (tc/assert-is-unprocessable-response response)))))))
