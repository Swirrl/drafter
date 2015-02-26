(ns drafter.configuration-test
  (:require [drafter.configuration :refer :all]
            [clojure.test :refer :all]))

(def selector-all (create-selector nil nil nil))

(deftest match-timeout-selector-test
  (testing "should not match"
    (are [input] (instance? Exception (match-timeout-selector input))
         :drafter-timeout-invalid
         :not-a-selector))

  (testing "should match"
    (are [input expected] (= expected (match-timeout-selector input))
         :drafter-timeout (create-selector nil nil nil)
         :drafter-timeout-endpoint-live (create-selector :live nil nil)
         :drafter-timeout-update (create-selector nil :update nil)
         :drafter-timeout-query (create-selector nil :query nil)
         :drafter-timeout-result (create-selector nil nil :result-timeout)
         :drafter-timeout-operation (create-selector nil nil :operation-timeout)
         :drafter-timeout-update-endpoint-raw (create-selector :raw :update nil)
         :drafter-timeout-query-endpoint-dump (create-selector :dump :query nil)
         :drafter-timeout-endpoint-live-result (create-selector :live nil :result-timeout)
         :drafter-timeout-endpoint-raw-operation (create-selector :raw nil :operation-timeout)
         :drafter-timeout-update-result (create-selector nil :update :result-timeout)
         :drafter-timeout-query-operation (create-selector nil :query :operation-timeout)
         :drafter-timeout-update-endpoint-live-result (create-selector :live :update :result-timeout))))

(deftest selector-lt?-test
  (are [s1 s2] (= true (selector-lt? s1 s2))
       selector-all (create-selector nil nil :result-timeout)
       selector-all (create-selector nil :query nil)
       selector-all (create-selector :live nil nil)
       (create-selector nil nil :result-timeout) (create-selector nil :update nil)
       (create-selector nil nil :operation-timeout) (create-selector :raw nil nil)
       (create-selector nil :query nil) (create-selector :dump nil nil)
       (create-selector nil :query :operation-timeout) (create-selector :live nil nil)
       (create-selector :live nil nil) (create-selector :live nil :result-timeout)
       (create-selector :raw nil :operation-timeout) (create-selector :raw :query nil)
       (create-selector :dump :update nil) (create-selector :dump :update :operation-timeout)))

(deftest try-parse-timeout-test
  (testing "non-numeric timeouts invalid"
    (is (instance? Exception (try-parse-timeout "abc"))))
  
  (testing "negative timeouts invalid"
    (is (instance? Exception (try-parse-timeout "-22"))))

  (testing "should convert valid to milliseconds"
    (is (= 34000 (try-parse-timeout "34")))))

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
      :drafter-timeout-endpoint-live-result "30" #{:raw :live} (create-setting (create-selector :live nil :result-timeout) 30000)
      :drafter-timeout-operation "10" #{:live} (create-setting (create-selector nil nil :operation-timeout) 10000))))

(deftest find-timeout-variables-test
  (let [setting1 (create-setting (create-selector :live :query nil) 10000)
        setting2 (create-setting (create-selector nil nil :result-timeout) 20000)
        endpoints #{:live :raw}
        env {:drafter-timeout-query-endpoint-live "10"
             :drafter-timeout-result "20"
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
