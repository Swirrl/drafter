(ns drafter.feature.draftset.show-test
  (:require [drafter.feature.draftset.show :as sut]
            [clojure.test :as t]))

;; TODO move from draftset-api-test
#_(t/deftest get-all-draftsets-changes-test
  (let [grouped-quads (group-by context (statements "test/resources/test-draftset.trig"))
        [graph1 graph1-quads] (first grouped-quads)
        [graph2 graph2-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api graph1-quads)

    ;;delete quads from graph1 and insert into graph2
    (delete-quads-through-api test-editor draftset-location (take 1 graph1-quads))
    (append-quads-to-draftset-through-api test-editor draftset-location graph2-quads)

    ;; Tests
    (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
      (t/is (= :updated (get-in changes [graph1 :status])))
      (t/is (= :created (get-in changes [graph2 :status]))))

    ;;delete graph1
    (delete-draftset-graph-through-api test-editor draftset-location graph1)
    (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
      (t/is (= :deleted (get-in changes [graph1 :status])))
      (t/is (= :created (get-in changes [graph2 :status]))))))
