(ns drafter.feature.draftset.list-test
  (:require [drafter.feature.draftset.list :as sut]
            [clojure.test :as t]
            [grafter.rdf :refer [add context statements]]
            [drafter.test-common :as tc]
            [drafter.util :as dutil]
            [drafter.swagger :as swagger]
            [drafter.routes.draftsets-api-test :as dset-test]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [grafter.rdf4j.io :as gio]
            [drafter.user :as user]
            [grafter.rdf4j.formats :as formats]
            [swirrl-server.async.jobs :as asjobs])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           java.net.URI
           java.util.Date))

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)

(defn get-draftsets-request [include user]
  (tc/with-identity user
    {:uri "/v1/draftsets" :request-method :get :params {:include include}}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TESTS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- assert-visibility [expected-draftset-names {:keys [body] :as response} message]
  (ok-response->typed-body [dset-test/Draftset] response) ;; check we get our draftset json back in an appropriate HTML response

  (let [draftset-names (set (map :display-name body))]
    (t/is (= expected-draftset-names draftset-names)
          message)))

(defn- assert-denies-invalid-login-attempts [handler request]
  (let [invalid-password {"Authorization" (str "Basic " (dutil/str->base64 "editor@swirrl.com:invalidpassword"))}
        invalid-user {"Authorization" (str "Basic " (dutil/str->base64 "nota@user.com:password"))}]

    (t/is (= 401 (:status (handler (assoc request :headers invalid-password)))))
    (t/is (= 401 (:status (handler (assoc request :headers invalid-user)))))))

(tc/deftest-system-with-keys get-all-draftsets-test
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-2.edn"]

  (assert-denies-invalid-login-attempts get-draftsets-handler {:uri "/v1/draftsets" :request-method :get :params {:include :all}})
  
  (assert-visibility #{"owned" "publishing"}
                     (get-draftsets-handler (get-draftsets-request :all test-publisher))
                     (str"Expected publisher to see owned and publishing draftsets"))

  (assert-visibility #{"editing" "publishing" "admining"}
                     (get-draftsets-handler (get-draftsets-request :all test-editor))
                     (str "Expected publisher to see owned and publishing draftsets"))

  (assert-visibility #{"publishing" "admining"}
                     (get-draftsets-handler (get-draftsets-request :all test-manager))
                     (str "Expected publisher to see owned and publishing draftsets")))

(tc/deftest-system-with-keys get-claimable-draftset-satisifes-multiple-claim-criteria
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  ;;a draftset may be in a state where it satisified multiple criteria to be claimed by
  ;;the current user e.g. if a user submits to a role they are in. In this case the user
  ;;can claim it due to being in the claim role, and because they are the submitter of
  ;;an unclaimed draftset. Drafter should only return any matching draftsets once in this
  ;;case

  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-1-draftset-with-submission.edn"]
  (let [{claimable-draftsets :body status :status} (get-draftsets-handler (get-draftsets-request :claimable test-manager))]
    (t/is (= 200 status))
    (t/is (= 1 (count claimable-draftsets)))))

(tc/deftest-system-with-keys get-claimable-draftset-satisifes-multiple-claim-criteria-2
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-1.edn"]

  ;; This test contains just a single empty draftset created by test-editor.
  
  (let [{all-draftsets :body} (get-draftsets-handler (get-draftsets-request :all test-editor))]
    (t/is (= 1 (count all-draftsets))))

  (let [{all-draftsets :body} (get-draftsets-handler (get-draftsets-request :all test-publisher))]
    (t/is (= 0 (count all-draftsets))
          "Other users can't access draftset as it's not been shared (submitted) to them")))

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

(tc/deftest-system-with-keys get-claimable-draftsets-changes-test
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/claimable-draftset-changes.edn"]
  (let [g1 (URI. "http://graph1")
        g2 (URI. "http://graph2")]

    (let [{ds-infos :body status :status } (get-draftsets-handler (get-draftsets-request :claimable test-publisher))
          grouped-draftsets (group-by :display-name ds-infos)
          ds1-info (first (grouped-draftsets "ds1"))
          ds2-info (first (grouped-draftsets "ds2"))]
           
      (t/is (= 200 status))
      (t/is (= :deleted (get-in ds1-info [:changes g1 :status])))
      (t/is (= :created (get-in ds2-info [:changes g2 :status]))))))


(defn- get-owned-draftsets-through-api [user]
  (get-draftsets-request :owned user))


;; NOTE this test is almost identical to the above one, we can probably combine them.
(tc/deftest-system-with-keys get-owned-draftsets-changes-test 
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/claimable-draftset-changes-2.edn"]

  (let [g1 (URI. "http://graph1")
        g2 (URI. "http://graph2")
        {ds-infos :body status :status} (get-draftsets-handler (get-draftsets-request :owned test-editor))
        {:keys [changes] :as ds-info} (first ds-infos)]
    
    (t/is (= 200 status))
    (t/is (= 1 (count ds-infos)))
    (t/is (= :deleted (get-in changes [g1 :status])))
    (t/is (= :created (get-in changes [g2 :status])))))

