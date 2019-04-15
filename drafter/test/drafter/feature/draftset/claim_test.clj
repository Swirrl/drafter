(ns drafter.feature.draftset.claim-test
  (:require [clojure.test :as t]
            [drafter.test-common :as tc]))

(t/use-fixtures :each tc/with-spec-instrumentation)

;; TODO

#_(defn- create-claim-request [draftset-location user]
  (tc/with-identity user {:uri (str draftset-location "/claim") :request-method :put}))

#_(defn- claim-draftset-through-api [draftset-location user]
  (let [claim-request (create-claim-request draftset-location user)
        {:keys [body] :as claim-response} (route claim-request)]
      (tc/assert-is-ok-response claim-response)
      (tc/assert-schema dset-test/Draftset body)
      body))

#_(t/deftest get-claimable-draftsets-test
  (let [ds-names (map #(str "Draftset " %) (range 1 6))
        [ds1 ds2 ds3 ds4 ds5] (doall (map #(create-draftset-through-api test-editor %) ds-names))]
    (submit-draftset-to-role-through-api test-editor ds1 :editor)
    (submit-draftset-to-role-through-api test-editor ds2 :publisher)
    (submit-draftset-to-role-through-api test-editor ds3 :manager)
    (submit-draftset-to-user-through-api ds5 test-publisher test-editor)

    ;;editor should be able to claim all draftsets just submitted as they have not been claimed
    (let [editor-claimable (get-claimable-draftsets-through-api test-editor)]
      (let [expected-claimable-names (map #(nth ds-names %) [0 1 2 4])
            claimable-names (map :display-name editor-claimable)]
        (t/is (= (set expected-claimable-names) (set claimable-names)))))

    (let [publisher-claimable (get-claimable-draftsets-through-api test-publisher)]
      ;;Draftsets 1, 2 and 5 should be on submit to publisher
      ;;Draftset 3 is in too high a role
      ;;Draftset 4 is not available
      (let [claimable-names (map :display-name publisher-claimable)
            expected-claimable-names (map #(nth ds-names %) [0 1 4])]
        (t/is (= (set expected-claimable-names) (set claimable-names)))))

    (doseq [ds [ds1 ds3]]
      (claim-draftset-through-api ds test-manager))

    (claim-draftset-through-api ds5 test-publisher)

    ;;editor should not be able to see ds1, ds3 or ds5 after they have been claimed
    (let [editor-claimable (get-claimable-draftsets-through-api test-editor)]
      (t/is (= 1 (count editor-claimable)))
      (t/is (= (:display-name (first editor-claimable)) (nth ds-names 1))))))
