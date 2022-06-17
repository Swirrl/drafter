(ns ^:rest-api drafter.feature.draftset.share-test
  (:require
   [clojure.test :as t :refer [is]]
   [drafter.draftset :as ds]
   [drafter.draftset.spec :as dss]
   [drafter.feature.draftset.test-helper :as help]
   [drafter.test-common :as tc]
   [drafter.user :as user]
   [drafter.user-test :refer [test-editor test-manager test-publisher]]))

(def keys-for-test [:drafter.fixture-data/loader [:drafter/routes :draftset/api]])

(tc/deftest-system-with-keys share-non-existent-draftset-with-permission
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
    (tc/assert-is-not-found-response
     (handler (help/create-share-with-permission-request
               test-editor "/v1/draftset/missing" :draft:claim))))

(tc/deftest-system-with-keys share-draftset-with-permission-by-non-owner
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
    (tc/assert-is-forbidden-response
     (handler (help/create-share-with-permission-request
               test-publisher
               (help/create-draftset-through-api handler test-editor)
               :draft:claim))))

(tc/deftest-system-with-keys share-draftset-with-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [{body :body :as share-response}
        (handler (help/share-draftset-with-user-request
                  (help/create-draftset-through-api handler test-editor)
                  test-publisher
                  test-editor))]
    (tc/assert-is-ok-response share-response)
    (tc/assert-spec ::ds/Draftset body)
    ;; Current owner doesn't change when sharing
    (is (= (user/username test-editor) (:current-owner body)))
    (is (= (user/username test-publisher) (:view-user body)))))

(tc/deftest-system-with-keys share-draftset-with-user-as-non-owner
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (tc/assert-is-forbidden-response
   (handler
    (help/share-draftset-with-user-request
     (help/create-draftset-through-api handler test-editor)
     test-manager
     test-publisher))))

(tc/deftest-system-with-keys share-non-existent-draftset-with-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (tc/assert-is-not-found-response
   (handler (help/share-draftset-with-user-request
             "/v1/draftset/missing" test-publisher test-editor))))

(tc/deftest-system-with-keys share-draftset-with-non-existent-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (tc/assert-is-unprocessable-response
   (handler (help/share-draftset-with-username-request
             (help/create-draftset-through-api handler test-editor)
             "invalid-user@example.com"
             test-editor))))

(tc/deftest-system-with-keys share-draftset-without-user-param
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        share-request (help/share-draftset-with-user-request
                       draftset-location test-publisher test-editor)
        share-request (update-in share-request [:params] dissoc :user)]
    (tc/assert-is-unprocessable-response (handler share-request))))

(tc/deftest-system-with-keys share-with-with-both-user-and-permission-params
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        request (help/share-draftset-with-user-request
                 draftset-location test-publisher test-editor)
        request (assoc-in request [:params :permission] "draft:claim")]
    (tc/assert-is-unprocessable-response (handler request))))

(tc/deftest-system-with-keys share-draftset-with-permission
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [{body :body :as share-response}
        (handler
         (help/create-share-with-permission-request
          test-editor
          (help/create-draftset-through-api handler test-editor)
          :draft:claim))]
    (tc/assert-is-ok-response share-response)
    (tc/assert-spec ::ds/Draftset body)
    ;; Current owner doesn't change when sharing
    (is (= (user/username test-editor) (:current-owner body)))
    (is (= :draft:claim (:view-permission body)))))
