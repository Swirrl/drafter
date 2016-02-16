(ns drafter.user-test
  (:require [drafter.user :refer :all]
            [clojure.test :refer :all]
            [drafter.draftset :as ds]
            [drafter.test-common :refer [test-editor test-publisher test-manager]])
  (:import [java.util UUID]))

(deftest has-role?-test
  (let [editor (create-user "editor@example.com" :editor "dslfkd")
        publisher (create-user "publisher@example.com" :publisher "xfsdfkzdjflksd")
        manager (create-user "manager@example.com" :manager "weirrsjtrj")]
    (are [user role has?] (= has? (has-role? user role))
         editor :editor true
         editor :publisher false
         editor :manager false

         publisher :editor true
         publisher :publisher true
         publisher :manager false

         manager :editor true
         manager :publisher true
         manager :manager true)))

(deftest authenticated?-test
  (let [api-key (str (UUID/randomUUID))
        user (create-user "test@example.com" :publisher api-key)]
    (are [user key should-authenticate?] (= should-authenticate? (authenticated? user key))
         user api-key true
         user "different key" false)))

(deftest is-owner?-test
  (are [user draftset expected] (= expected (is-owner? user draftset))
       test-editor (ds/create-draftset (username test-editor)) true
       test-editor (ds/create-draftset (username test-publisher)) false))

(deftest can-claim?-test
  (are [user draftset expected] (= expected (can-claim? user draftset))
       ;;owned by self
       test-editor (ds/create-draftset (username test-editor)) true

       ;;owned by other
       test-publisher (ds/create-draftset (username test-editor)) false

       ;;in claimable role
       test-manager (-> (ds/create-draftset "tmp") (ds/offer-to :publisher)) true

       ;;not in claimable role
       test-editor (-> (ds/create-draftset "tmp") (ds/offer-to :publisher)) false))

(deftest permitted-draftset-operations-test
  (are [user draftset expected-operations] (= expected-operations (permitted-draftset-operations draftset user))
       ;;non-publishing owner
       test-editor (ds/create-draftset (username test-editor)) #{:delete :edit :offer}

       ;;publishing owner
       test-publisher (ds/create-draftset (username test-publisher)) #{:delete :edit :offer :publish}

       ;;non-owner
       test-publisher (ds/create-draftset (username test-editor)) #{}

       ;;user in claim role
       test-publisher (-> (ds/create-draftset "tmp@example.com") (ds/offer-to :publisher)) #{:claim}

       ;;user not in claim role
       test-editor (-> (ds/create-draftset "tmp@example.com") (ds/offer-to :manager)) #{}))
