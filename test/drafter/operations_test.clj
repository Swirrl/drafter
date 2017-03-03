(ns drafter.operations-test
  (:require [drafter.operations :refer :all]
            [clojure.test :refer :all]
            [schema.test :refer [validate-schemas]])

  (:import [java.util.concurrent Future FutureTask Executors Executor TimeUnit]
           [java.nio.charset Charset]
           [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :each validate-schemas)

(defn fixed-clock [at] {:now-fn (constantly at)})

(defn cancel-only-future []
  (let [cancelled (AtomicBoolean. false)
        invalid-fn (fn [] (throw (RuntimeException. "Can't get result")))]
    (reify
      Future
      (get [_] (invalid-fn))
      (get [_ timeout unit] (invalid-fn))
      (isCancelled [_] (.get cancelled))
      (isDone [_] (.get cancelled))
      (cancel [_ interrupt?]
        (.set cancelled true)
        true))))

(defn completed-future []
  (reify
    Future
    (get [_] nil)
    (get [_ timeout unit] nil)
    (isCancelled [_] false)
    (isDone [_] true)
    (cancel [_ interrupt?] false)))

(defn current-thread-executor []
  (reify
    Executor
    (execute [_ r] (.run r))))

(deftest max-timestamp-test
  (let [first-result 100
        second-result 200]
    (are [e1 e2 max] (= max (max-timestamp e1 e2))
         nil first-result first-result
         first-result nil first-result
         first-result second-result second-result
         second-result first-result second-result)))

(deftest timed-out-test
  (let [initial-state (assoc (create-timeouts 1000) :started-at 100)
        set-last-timestamp (fn [e] (assoc initial-state :timestamp e))]
    (are [timestamp now should-be-timed-out?]
      (= should-be-timed-out? (timed-out? (fixed-clock now) (set-last-timestamp timestamp)))
         nil 150 false  ;first result not yet exceeded timeout
         150 200 false  ;next result not yet exceeded timeout
         1150 1200 true ;total timeout exceeded
         )))

(deftest get-status-p-test
  (are [operation operation-state timed-out-p expected-state] (= expected-state (get-status-p timed-out-p operation operation-state))
       (completed-future) {} (constantly true) :completed
       (cancel-only-future) {} (constantly true) :timed-out
       (cancel-only-future) {:timestamp nil} (constantly false) :in-progress
       (cancel-only-future) {:timestamp 200} (constantly false) :in-progress))

;group-by :: [a] -> (a -> k) -> Map[k, [a]]
(deftest categorise-f-test
  (let [m {:op1 {:key :a}
           :op2 {:key :b}
           :op3 {:key :a}
           :op4 {:key :c}}
        categorised (categorise-f m (fn [k v] (:key v)))
        expected {:a {:op1 {:key :a}
                      :op3 {:key :a}}
                  :b {:op2 {:key :b}}
                  :c {:op4 {:key :c}}}]
    (is (= expected categorised))))

(deftest process-categorised-test
  (let [timed-out1 (atom (cancel-only-future))
        timed-out2 (atom (cancel-only-future))
        in-progress1 (atom (cancel-only-future))
        completed1 (atom (completed-future))
        categorised {:timed-out {timed-out1 {} timed-out2 {}} :in-progress {in-progress1 {}} :completed {completed1 {}}}
        remaining (process-categorised categorised)]

    (testing "should cancel timed-out operations"
      (is (= true (.isCancelled @timed-out1)))
      (is (= true (.isCancelled @timed-out2))))

    (testing "should only keep in-progress operations"
      (is (= {in-progress1 {}} remaining)))))

(deftest register-for-cancellation-on-timeout-test
  (let [operations (atom {})
        now 100
        timeouts (create-timeouts 5000)
        expected-state (assoc timeouts :started-at now)
        fut (FutureTask. (fn [] 1))]

    (register-for-cancellation-on-timeout fut timeouts operations (fixed-clock now))

    (let [[k state] (first @operations)]
      (is (= 1 (count @operations)))
      (is (= fut @k))
      (is (= expected-state state)))))

(deftest reaper-fn-test
  (let [[task2 task3] (take 2 (repeatedly cancel-only-future))
        task4 (completed-future)
        checked-at 1500
        operations {(atom task2) {:started-at 100 :result-timeout 200  :operation-timeout 1000  :timestamp 1400}
                    (atom task3) {:started-at 100 :result-timeout 500  :operation-timeout 10000 :timestamp 1200}
                    (atom task4) {:started-at 100 :result-timeout 200  :operation-timeout 1000  :timestamp 1400}}
        operations-atom (atom operations)
        reaper-fn (create-reaper-fn operations-atom (fixed-clock checked-at))]

    (reaper-fn)

    ;task2 has exceeded the total operation timeout
    ;task3 has not timed out and is still in progress
    ;task4 has completed normally
    (let [survived @operations-atom]
      (is (= [task3] (vec (map (fn [r] @r) (keys survived)))))
      (is (.isCancelled task2)))))

