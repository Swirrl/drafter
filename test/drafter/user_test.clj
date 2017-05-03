(ns drafter.user-test
  (:require [clojure.test :refer :all]
            [drafter
             [draftset :as ds]
             [user :refer :all]])
  (:import clojure.lang.ExceptionInfo
           java.util.UUID))

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

(deftest password-valid?-test
  (let [password (str (UUID/randomUUID))
        password-digest (get-digest password)
        user (create-user "test@example.com" :publisher password-digest)]
    (are [key valid?] (= valid? (password-valid? user key))
         password true
         (get-digest "different password") false)))

(deftest validate-token!-test
  (testing "Invalid email"
    (is (thrown? ExceptionInfo (validate-token! {:email "not an email address" :role "publisher"}))))

  (testing "Invalid role"
    (is (thrown? ExceptionInfo (validate-token! {:email "foo@bar.com" :role "invalid"}))))

  (testing "Valid token"
    (is (= {:email "foo@bar.com" :role :editor} (validate-token! {:email "foo@bar.com" :role "editor"})))))

(deftest authenticated!-test
  (are [user expected] (= expected (authenticated! user))
    (create-user "test@example.com" :publisher "digest") (create-authenticated-user "test@example.com" :publisher)
    (create-authenticated-user "test@example.com" :editor) (create-authenticated-user "test@example.com" :editor)
    (->
     (create-authenticated-user "test@example.com" :editor)
     (assoc :x "x")
     (assoc :y "y")) (create-authenticated-user "test@example.com" :editor)))

(deftest is-owner?-test
  (are [user draftset expected] (= expected (is-owner? user draftset))
       test-editor (ds/create-draftset (username test-editor)) true
       test-editor (ds/create-draftset (username test-publisher)) false))

(defn- submitted-to-role [creator role]
  (-> (ds/create-draftset (username creator))
      (ds/submit-to-role (username creator) role)))

(defn- submitted-to-user [creator target]
  (-> (ds/create-draftset (username creator))
      (ds/submit-to-user (username creator) (username target))))

(deftest is-submitted-by?-test
  (are [user draftset expected] (= expected (is-submitted-by? user draftset))
    test-editor (submitted-to-role test-editor :publisher) true
    test-publisher (submitted-to-role test-editor :manager) false
    test-editor (ds/create-draftset (username test-editor)) false))

(deftest can-claim?-test
  (are [user draftset expected] (= expected (can-claim? user draftset))
    ;;owner can claim their own draftset
    test-editor (ds/create-draftset (username test-editor)) true

    ;;submitter can re-claim draftset if it has not yet been claimed
    test-editor (submitted-to-role test-editor :publisher) true
    test-editor (submitted-to-user test-editor test-manager) true

    ;;user can claim draftset submitted to their role
    test-publisher (submitted-to-role test-editor :publisher) true

    ;;user can claim draftset submitted to them
    test-manager (submitted-to-user test-editor test-manager) true

    ;;user cannot claim draftset owned by another user
    test-publisher (ds/create-draftset (username test-editor)) false

    ;;user cannot claim draftset submitted to role they are not in
    test-publisher (submitted-to-role test-editor :manager) false

    ;;user cannot claim draftset submitted to other user
    test-manager (submitted-to-user test-editor test-publisher) false))

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
