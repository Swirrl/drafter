(ns ^:rest-api drafter.feature.draftset.share-test
  (:require
   [clojure.test :as t :refer [is]]
   [drafter.draftset :as ds]
   [drafter.draftset.spec :as dss]
   [drafter.feature.draftset.query-test :as query]
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
               test-editor "/v1/draftset/missing" :draft:view))))

(tc/deftest-system-with-keys share-draftset-with-permission-by-non-owner
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
    (tc/assert-is-forbidden-response
     (handler (help/create-share-with-permission-request
               test-publisher
               (help/create-draftset-through-api handler test-editor)
               :draft:view))))

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
    (is (= #{(user/username test-publisher)} (:view-users body)))))

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
        request (assoc-in request [:params :permission] "draft:view")]
    (tc/assert-is-unprocessable-response (handler request))))

(tc/deftest-system-with-keys share-draftset-with-permission
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [{body :body :as share-response}
        (handler
         (help/create-share-with-permission-request
          test-editor
          (help/create-draftset-through-api handler test-editor)
          :draft:view))]
    (tc/assert-is-ok-response share-response)
    (tc/assert-spec ::ds/Draftset body)
    ;; Current owner doesn't change when sharing
    (is (= (user/username test-editor) (:current-owner body)))
    (is (= #{:draft:view} (:view-permissions body)))))

(tc/deftest-system-with-keys share-draftset-with-multiple-users-and-permissions
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset (help/create-draftset-through-api handler test-manager)]
    (tc/assert-is-ok-response
     (handler (help/create-share-with-permission-request
               test-manager draftset :draft:view)))
    (tc/assert-is-ok-response
     (handler (help/create-share-with-permission-request
               test-manager draftset :draft:view:special)))
    (tc/assert-is-ok-response
     (handler (help/share-draftset-with-user-request
               draftset test-publisher test-manager)))
    (let [res (handler (help/share-draftset-with-user-request
                        draftset test-editor test-manager))]
      (tc/assert-is-ok-response res)
      (is (= #{:draft:view :draft:view:special}
             (:view-permissions (:body res)))
      (is (= #{"publisher@swirrl.com" "editor@swirrl.com"}
             (:view-users (:body res))))))))

(tc/deftest-system-with-keys can-query-draftset-shared-with-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset (help/create-draftset-through-api handler test-manager)]
    (tc/assert-is-ok-response
     (handler (help/share-draftset-with-user-request draftset
                                                     test-editor
                                                     test-manager)))
    (tc/assert-is-ok-response
     (handler (help/get-draftset-quads-request draftset test-editor :nq "true")))
    (tc/assert-is-ok-response
     (handler
      (query/create-query-request test-editor
                                  draftset
                                  "select * where { ?s ?p ?o }"
                                  "application/sparql-results+json"
                                  :union-with-live? "true")))))

(tc/deftest-system-with-keys can-query-draftset-shared-with-permission
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset (help/create-draftset-through-api handler test-manager)]
    (tc/assert-is-ok-response
     (handler (help/create-share-with-permission-request
               test-manager draftset :draft:view)))
    (tc/assert-is-ok-response
     (handler (help/get-draftset-quads-request draftset test-editor :nq "true")))
    (tc/assert-is-ok-response
     (handler
      (query/create-query-request test-editor
                                  draftset
                                  "select * where { ?s ?p ?o }"
                                  "application/sparql-results+json"
                                  :union-with-live? "true")))))
