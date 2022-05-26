(ns ^:rest-api drafter.feature.draftset.claim-test
  (:require [clojure.test :as t :refer [is]]
            [drafter.draftset :as ds]
            [drafter.draftset.spec :as dss]
            [drafter.feature.draftset.create-test :as ct]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test
             :refer [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn- create-claim-request [draftset-location user]
  (tc/with-identity user {:uri (str draftset-location "/claim") :request-method :put}))

(defn- claim-draftset-through-api [handler draftset-location user]
  (let [claim-request (create-claim-request draftset-location user)
        {:keys [body] :as claim-response} (handler claim-request)]
    (tc/assert-is-ok-response claim-response)
    (tc/assert-spec ::ds/Draftset body)
      body))

(defn- submit-draftset-to-username-request [draftset-location target-username user]
  (tc/with-identity user
    {:uri (str draftset-location "/submit-to") :request-method :post :params {:user target-username}}))

(defn- submit-draftset-to-user-request [draftset-location target-user user]
  (submit-draftset-to-username-request draftset-location (user/username target-user) user))

(defn- submit-draftset-to-user-through-api [handler draftset-location target-user user]
  (let [request (submit-draftset-to-user-request draftset-location target-user user)
        response (handler request)]
    (tc/assert-is-ok-response response)))

(def keys-for-test [:drafter.fixture-data/loader [:drafter/routes :draftset/api]])

(tc/deftest-system-with-keys claim-draftset-submitted-to-permission
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (help/submit-draftset-to-permission-through-api handler
                                                    test-editor
                                                    draftset-location
                                                    :draft:claim)
    (let [{:keys [current-owner] :as ds-info}
          (claim-draftset-through-api handler draftset-location test-publisher)]
      (is (= (user/username test-publisher) current-owner)))))

;; The role parameter is deprecated
(tc/deftest-system-with-keys claim-draftset-submitted-to-role
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (help/submit-draftset-to-role-through-api handler
                                              test-editor
                                              draftset-location
                                              :editor)
    (let [{:keys [current-owner] :as ds-info}
          (claim-draftset-through-api handler draftset-location test-publisher)]
      (is (= (user/username test-publisher) current-owner)))))

(tc/deftest-system-with-keys claim-draftset-submitted-to-user
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (submit-draftset-to-user-through-api handler draftset-location test-publisher test-editor)
    (let [{:keys [current-owner claim-user] :as ds-info}
          (claim-draftset-through-api handler draftset-location test-publisher)]
      (is (= (user/username test-publisher) current-owner))
      (is (nil? claim-user)))))

(tc/deftest-system-with-keys claim-draftset-submitted-to-other-user
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (submit-draftset-to-user-through-api handler draftset-location test-publisher test-editor)
    (let [claim-request (create-claim-request draftset-location test-manager)
          claim-response (handler claim-request)]
      (tc/assert-is-forbidden-response claim-response))))

(tc/deftest-system-with-keys claim-draftset-owned-by-self
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))
        claim-request (create-claim-request draftset-location test-editor)
        {:keys [body] :as claim-response} (handler claim-request)]
    (tc/assert-is-ok-response claim-response)
    (is (= (user/username test-editor) (:current-owner body)))))

(tc/deftest-system-with-keys claim-unowned-draftset-submitted-by-self
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (help/submit-draftset-to-permission-through-api handler
                                                    test-editor
                                                    draftset-location
                                                    :draft:claim)
    (claim-draftset-through-api handler draftset-location test-editor)))

(tc/deftest-system-with-keys claim-owned-by-other-user-draftset-submitted-by-self
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (help/submit-draftset-to-permission-through-api handler
                                                    test-editor
                                                    draftset-location
                                                    :draft:claim)
    (claim-draftset-through-api handler draftset-location test-publisher)
    (let [response (handler (create-claim-request draftset-location test-editor))]
      (tc/assert-is-forbidden-response response))))

(tc/deftest-system-with-keys claim-draftset-owned-by-other-user
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))
        claim-request (create-claim-request draftset-location test-publisher)
        claim-response (handler claim-request)]
    (tc/assert-is-forbidden-response claim-response)))

(tc/deftest-system-with-keys ^:basic-auth claim-draftset-by-user-without-permission
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter.user/memory-repository]
  [{handler [:drafter/routes :draftset/api]
    users :drafter.user/memory-repository :as system} "test-system.edn"]
  (let [other-editor (user/create-user "edtheduck@example.com" :editor (user/get-digest test-password))
        handler (get system [:drafter/routes :draftset/api])
        {{draftset-location "Location"} :headers}
        (handler (ct/create-draftset-request test-editor))]
    (memrepo/add-user users other-editor)
    (help/submit-draftset-to-permission-through-api handler
                                                    test-editor
                                                    draftset-location
                                                    :draft:publish)
    (let [claim-response (handler (create-claim-request draftset-location other-editor))]
      (tc/assert-is-forbidden-response claim-response))))

(tc/deftest-system-with-keys claim-non-existent-draftset
  keys-for-test
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        claim-response (handler (create-claim-request "/v1/draftset/missing" test-publisher))]
    (tc/assert-is-not-found-response claim-response)))
