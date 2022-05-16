(ns drafter.user-test
  (:require [clojure.test :refer :all :as t]
            [drafter.draftset :as ds]
            [drafter.test-common :as tc]
            [drafter.user :refer :all])
  (:import clojure.lang.ExceptionInfo
           java.util.UUID))

(use-fixtures :each tc/with-spec-instrumentation)

(def test-password "password")
(def test-norole (create-user "norole@swirrl.com" :norole (get-digest test-password)))
(def test-access (create-user "access@swirrl.com" :access (get-digest test-password)))
(def test-editor (create-user "editor@swirrl.com" :editor (get-digest test-password)))
(def test-publisher (create-user "publisher@swirrl.com" :publisher (get-digest test-password)))
(def test-manager (create-user "manager@swirrl.com" :manager (get-digest test-password)))
(def test-system (create-user "system@swirrl.com" :system (get-digest test-password)))

(deftest create-user-test
  (testing "Invalid email address"
    (is (thrown? IllegalArgumentException (create-user "invalidemail" :publisher (get-digest "password"))))))

(deftest has-permission?-test
  (are [user permission has?] (= has? (has-permission? user permission))
       test-editor :draft:edit true
       test-editor :draft:publish false
       test-editor :draft:publish false

       test-publisher :draft:edit true
       test-publisher :draft:publish true
       test-publisher :draft:publish true

       test-manager :draft:edit true
       test-manager :draft:publish true
       test-manager :draft:publish true))

(t/deftest roles-including-test
  (t/are [role expected] (= expected (roles-including role))
    :access #{:access :editor :publisher :manager :system}
    :editor #{:editor :publisher :manager :system}
    :publisher #{:publisher :manager :system}
    :manager #{:manager :system}
    :system #{:system}))

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
    (is (= {:email "foo@bar.com"
            :permissions (role->permissions :editor)}
           (validate-token! {:email "foo@bar.com" :role "editor"})))))

(deftest authenticated!-test
  (are [user expected] (= expected (authenticated! user))
    (create-user "test@example.com" :publisher "digest")
    (create-authenticated-user "test@example.com"
                               (role->permissions :publisher))

    (create-authenticated-user "test@example.com" (role->permissions :editor))
    (create-authenticated-user "test@example.com" (role->permissions :editor))

    (assoc (create-authenticated-user "test@example.com"
                                      (role->permissions :editor))
           :x "x" :y "y")
    (create-authenticated-user "test@example.com"
                               (role->permissions :editor))))

(deftest is-owner?-test
  (are [user draftset expected] (= expected (is-owner? user draftset))
       test-editor (ds/create-draftset (username test-editor)) true
       test-editor (ds/create-draftset (username test-publisher)) false))

(defn- submitted-to-permission [creator permission]
  (-> (ds/create-draftset (username creator))
      (ds/submit-to-permission (username creator) permission)))

(defn- submitted-to-user [creator target]
  (-> (ds/create-draftset (username creator))
      (ds/submit-to-user (username creator) (username target))))

(deftest is-submitted-by?-test
  (are [user draftset expected] (= expected (is-submitted-by? user draftset))
    test-editor (submitted-to-permission test-editor :draft:claim) true
    test-publisher (submitted-to-permission test-editor :draft:claim) false
    test-editor (ds/create-draftset (username test-editor)) false))

(deftest can-claim?-test
  (are [user draftset expected] (= expected (can-claim? user draftset))
    ;;owner can claim their own draftset
    test-editor (ds/create-draftset (username test-editor)) true

    ;;submitter can re-claim draftset if it has not yet been claimed
    test-editor (submitted-to-permission test-editor :draft:claim) true
    test-editor (submitted-to-user test-editor test-manager) true

    ;;user can claim draftset submitted to a permission they have
    test-publisher (submitted-to-permission test-editor :draft:claim) true

    ;;user can claim draftset submitted to them
    test-manager (submitted-to-user test-editor test-manager) true

    ;;user cannot claim draftset owned by another user
    test-publisher (ds/create-draftset (username test-editor)) false

    ;;user cannot claim draftset submitted to a permission they don't have
    test-publisher (submitted-to-permission test-editor :draft:claim:manager) false

    ;;user cannot claim draftset submitted to other user
    test-manager (submitted-to-user test-editor test-publisher) false))

(deftest permitted-draftset-operations-test
  (are [user draftset expected-operations] (= expected-operations (permitted-draftset-operations draftset user))
    ;;non-publishing owner
    test-editor
    (ds/create-draftset (username test-editor))
    #{:share :claim :delete :edit :view :create :submit}

    ;;publishing owner
    test-publisher
    (ds/create-draftset (username test-publisher))
    #{:share :claim :delete :edit :view :create :submit :publish}

    ;;submitter on unclaimed
    test-editor (submitted-to-permission test-editor :draft:claim) #{:claim}

    ;;submitted on claimed
    test-editor
    (ds/claim (submitted-to-permission test-editor :draft:claim) test-publisher)
    #{}

    ;;non-owner
    test-publisher (ds/create-draftset (username test-editor)) #{}

    ;;user has claim permission
    test-publisher
    (ds/submit-to-permission (ds/create-draftset "tmp@example.com")
                             "tmp@example.com"
                             :draft:claim)
    #{:claim}

    ;;user doesn't have claim permission
    test-editor
    (ds/submit-to-permission (ds/create-draftset "tmp@example.com")
                             "tmp@example.com"
                             :draft:publish)
    #{}))
