(ns drafter.user-test
  (:require [drafter.user :refer :all]
            [clojure.test :refer :all])
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
