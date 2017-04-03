(ns drafter.configuration-test
  (:require [drafter.configuration :refer :all]
            [clojure.test :refer :all]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each validate-schemas)

(def selector-all (create-selector nil nil))

(deftest match-timeout-selector-test
  (testing "should not match"
    (are [input] (instance? Exception (match-timeout-selector input))
         :drafter-timeout-invalid
         :not-a-selector))

  (testing "should match"
    (are [input expected] (= expected (match-timeout-selector input))
         :drafter-timeout (create-selector nil nil)
         :drafter-timeout-endpoint-live (create-selector :live nil)
         :drafter-timeout-update (create-selector nil :update)
         :drafter-timeout-query (create-selector nil :query)
         :drafter-timeout-update-endpoint-raw (create-selector :raw :update)
         :drafter-timeout-query-endpoint-dump (create-selector :dump :query)
         :drafter-timeout-endpoint-raw (create-selector :raw nil))))

(deftest selector-lt?-test
  (are [s1 s2] (= true (selector-lt? s1 s2))
       selector-all (create-selector nil :query)
       selector-all (create-selector :live nil)
       (create-selector nil nil) (create-selector :raw nil)
       (create-selector nil :query) (create-selector :dump nil)
       (create-selector nil :query) (create-selector :live nil)
       (create-selector :raw nil) (create-selector :raw :query)))

(deftest parse-and-validate-timeout-setting-test
  (testing "invalid settings"
    (are [name value endpoints]
      (instance? Exception (parse-and-validate-timeout-setting name value endpoints))
      ;invalid setting format
      :drafter-timeout-invalid "23" #{:live}

      ;invalid timeout
      :drafter-timeout-result "abc" #{:live}

      ;invalid timeout
      :drafter-timeout-operation "-34" #{:live}

      ;unknown endpoint name
      :drafter-timeout-endpoint-raw "20" #{:live}

      ;result timeout for update endpoint
      :drafter-timeout-update-result "10" #{:live}))

  (testing "valid settings"
    (are [name value endpoints expected]
      (= expected (parse-and-validate-timeout-setting name value endpoints))
      :drafter-timeout-endpoint-live "30" #{:raw :live} (create-setting (create-selector :live nil) 30)
      :drafter-timeout "10" #{:live} (create-setting (create-selector nil nil) 10))))

(deftest find-timeout-variables-test
  (let [setting1 (create-setting (create-selector :live :query) 10)
        setting2 (create-setting (create-selector nil :update) 20)
        endpoints #{:live :raw}
        env {:drafter-timeout-query-endpoint-live "10"
             :drafter-timeout-update "20"
             :drafter-timeout-endpoint-nonexistent "50"
             :other-setting "other-value"
             :drafter-timeout-invalid "invalid"
             :drafter-timeout-operation "not-a-timeout"}
        {:keys [errors settings]} (find-timeout-variables env endpoints)]

    (is (= 3 (count errors)))
    (is (= #{setting1 setting2} (set settings)))))

(deftest step-test
  (are [path next tree expected] (= (set expected) (set (step path next tree)))
       [] :a {:a 1} [[[:a] 1]]
       [:f] nil {:a 1 :b 2} [[[:f :a] 1] [[:f :b] 2]]
       [] :c {:a 1 :b 2} nil))

(deftest find-paths-test
  (let [tree {:name1 {:first {:a 1 :b 2}
                      :second {:a 5}}
              :name2 {:first {:c 3 :d 4}
                      :second {:a 10 :e 4}}}]
    (are [spec paths] (= (set paths) (set (find-paths tree spec)))
         [:name2 :second :e] [[[:name2 :second :e] 4]]
         [:name1 :first nil] [[[:name1 :first :a] 1] [[:name1 :first :b] 2]]
         [:name1 nil :a] [[[:name1 :first :a] 1] [[:name1 :second :a] 5]]
         [:name1 nil nil] [[[:name1 :first :a] 1] [[:name1 :first :b] 2] [[:name1 :second :a] 5]]
         [nil :second :a] [[[:name1 :second :a] 5] [[:name2 :second :a] 10]]
         [nil :second nil] [[[:name1 :second :a] 5] [[:name2 :second :a] 10] [[:name2 :second :e] 4]]
         [nil nil :b] [[[:name1 :first :b] 2]]
         [nil nil nil] [[[:name1 :first :a] 1]
                        [[:name1 :first :b] 2]
                        [[:name1 :second :a] 5]
                        [[:name2 :first :c] 3]
                        [[:name2 :first :d] 4]
                        [[:name2 :second :a] 10]
                        [[:name2 :second :e] 4]])))

(deftest update-paths-test
  (let [source {:name1 {:first {:a 1 :b 2}
                        :second {:c 3 :d 4}}
                :name2 {:first {:e 5}}}
        path1 [:name1 :first :b]
        path2 [:name2 :first :e]
        value1 55
        value2 66
        expected {:name1 {:first {:a 1 :b value1}
                          :second {:c 3 :d 4}}
                  :name2 {:first {:e value2}}}
        updated (update-paths source [[path1 value1] [path2 value2]])]
    (is (= expected updated))))
