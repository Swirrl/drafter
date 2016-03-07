(ns drafter.user-test
  (:require [drafter.user :refer :all]
            [clojure.test :refer :all]
            [drafter.draftset :as ds])
  (:import [java.util UUID]))

(deftest has-role?-test
  (are [user role has?] (= has? (has-role? user role))
       test-editor :editor true
       test-editor :publisher false
       test-editor :manager false

       test-publisher :editor true
       test-publisher :publisher true
       test-publisher :manager false

       test-manager :editor true
       test-manager :publisher true
       test-manager :manager true))

(deftest authenticated?-test
  (let [api-key (str (UUID/randomUUID))
        api-key-digest (get-digest api-key)
        user (create-user "test@example.com" :publisher api-key-digest)]
    (are [user key should-authenticate?] (= should-authenticate? (authenticated? user key))
         user api-key true
         user (get-digest "different key") false)))

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
       test-manager (-> (ds/create-draftset "tmp") (ds/submit-to :publisher)) true

       ;;not in claimable role
       test-editor (-> (ds/create-draftset "tmp") (ds/submit-to :publisher)) false))

(deftest permitted-draftset-operations-test
  (are [user draftset expected-operations] (= expected-operations (permitted-draftset-operations draftset user))
       ;;non-publishing owner
    test-editor (ds/create-draftset (username test-editor)) #{:delete :edit :submit}

       ;;publishing owner
    test-publisher (ds/create-draftset (username test-publisher)) #{:delete :edit :submit :publish}

    ;;non-owner
    test-publisher (ds/create-draftset (username test-editor)) #{}

    ;;user in claim role
    test-publisher (-> (ds/create-draftset "tmp@example.com") (ds/submit-to :publisher)) #{:claim}

    ;;user not in claim role
    test-editor (-> (ds/create-draftset "tmp@example.com") (ds/submit-to :manager)) #{}))
