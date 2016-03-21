(ns drafter.user-test
  (:require [drafter.user :refer :all]
            [clojure.test :refer :all]
            [drafter.draftset :as ds])
  (:import [java.util UUID]))

(def test-password "password")
(def test-editor (create-user "editor@example.com" :editor (get-digest test-password)))
(def test-publisher (create-user "publisher@example.com" :publisher (get-digest test-password)))
(def test-manager (create-user "manager@example.com" :manager (get-digest test-password)))

(deftest create-user-test
  (testing "Invalid email address"
    (is (thrown? IllegalArgumentException (create-user "invalidemail" :publisher (get-digest "password"))))))

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

(defn- submitted-to-role [owner role]
  (-> (ds/create-draftset (username owner))
      (ds/submit-to-role (username owner) role)))

(deftest is-submitted-by?-test
  (are [user draftset expected] (= expected (is-submitted-by? user draftset))
    test-editor (submitted-to-role test-editor :publisher) true
    test-publisher (submitted-to-role test-editor :manager) false
    test-editor (ds/create-draftset (username test-editor)) false))

(deftest permitted-draftset-operations-test
  (are [user draftset expected-operations] (= expected-operations (permitted-draftset-operations draftset user))
    ;;non-publishing owner
    test-editor (ds/create-draftset (username test-editor)) #{:delete :edit :submit :claim}

    ;;publishing owner
    test-publisher (ds/create-draftset (username test-publisher)) #{:delete :edit :submit :claim :publish}

    ;;submitter on unclaimed
    test-editor (submitted-to-role test-editor :publisher) #{:claim}

    ;;submitted on claimed
    test-editor (ds/claim (submitted-to-role test-editor :publisher) test-publisher) #{}

    ;;non-owner
    test-publisher (ds/create-draftset (username test-editor)) #{}

    ;;user in claim role
    test-publisher (ds/submit-to-role (ds/create-draftset "tmp@example.com") "tmp@example.com" :publisher) #{:claim}

    ;;user not in claim role
    test-editor (ds/submit-to-role (ds/create-draftset "tmp@example.com") "tmp@example.com" :manager) #{}))
