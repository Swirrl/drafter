(ns drafter.user-test
  (:require [drafter.user :refer :all]
            [clojure.test :refer :all]))

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
