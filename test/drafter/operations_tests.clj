(ns drafter.operations-tests
  (:require [drafter.operations :refer :all]
            [clojure.test :refer :all])
  (:import [java.util.concurrent Future Executors Executor TimeUnit]
           [java.nio.charset Charset]
           [java.util.concurrent.atomic AtomicBoolean]))

(defn fixed-clock [at] {:now-fn (constantly at) :offset-fn +})

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

(deftest max-event-test
  (let [first-result 100
        second-result 200]
    (are [e1 e2 max] (= max (max-event e1 e2))
         nil first-result first-result
         first-result nil first-result
         first-result second-result second-result
         second-result first-result second-result)))

(deftest update-lastest-operation-event-test
  (let [operation-map {:op {:last-event 100}}]
    (testing "should update latest event for known operation"
      (let [next-event 200
            updated-map (update-latest-operation-event operation-map :op next-event)]
        (is (= {:op {:last-event next-event}} updated-map))))

    (testing "should ignore event for unknown operation"
      (let [updated-map (update-latest-operation-event operation-map :unknown 300)]
        (is (= operation-map updated-map))))))

(deftest timed-out-test
  (let [initial-state (assoc (create-timeouts 100 1000) :started-at 100)
        set-last-event (fn [e] (assoc initial-state :last-event e))]
    (are [last-event now should-be-timed-out?] (= should-be-timed-out? (timed-out? (fixed-clock now) (set-last-event last-event)))
         nil 250 true   ;first result exceeded timeout
         nil 150 false  ;first result not yet exceeded timeout
         150 200 false  ;next result not yet exceeded timeout
         150 300 true   ;next result exceeded timeout
         1150 1200 true ;next result not timed out but total timeout exceeded
         )))

(deftest get-status-p-test
  (are [operation operation-state timed-out-p expected-state] (= expected-state (get-status-p timed-out-p operation operation-state))
       (completed-future) {} (constantly true) :completed
       (cancel-only-future) {} (constantly true) :timed-out
       (cancel-only-future) {:last-event nil} (constantly false) :in-progress
       (cancel-only-future) {:last-event 200} (constantly false) :in-progress))

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

(deftest create-operation-publish-fn-test
  (let [now 200
        key :op
        operations (atom {:op {}})
        publish-fn (create-operation-publish-fn :op (fixed-clock now) operations)]
    
    (publish-fn)

    (let [last-event (get-in @operations [:op :last-event])]
      (is (= now last-event)))))

(deftest register-operation-test
  (testing "should register operation and publish event"
    (let [now 100
          result-timeout 200
          operation-timeout 1000
          operations (atom {})
          {:keys [publish operation-ref] :as operation} (create-operation operations (fixed-clock now))]
      (connect-operation operation (fn []))
      (register-operation operation (create-timeouts result-timeout operation-timeout))

      (publish)

      (let [last-event (get-in @operations [operation-ref :last-event])]
        (is (= now last-event))))))

(deftest submit-operation-test
  (let [operations (atom {})
        clock (fixed-clock 100)
        ran-op (atom false)
        operation (create-operation operations clock)]
    
    (connect-operation operation (fn [] (reset! ran-op true)))
    (submit-operation operation (current-thread-executor) (create-timeouts 100 1000))

    (is (= true @ran-op))))

(deftest reaper-fn-test
  (let [[task1 task2 task3] (take 3 (repeatedly cancel-only-future))
        task4 (completed-future)
        checked-at 1500
        operations {(atom task1) {:started-at 100 :result-timeout 1000 :operation-timeout 10000}
                    (atom task2) {:started-at 100 :result-timeout 200  :operation-timeout 1000  :last-event 1400}
                    (atom task3) {:started-at 100 :result-timeout 500  :operation-timeout 10000 :last-event 1200}
                    (atom task4) {:started-at 100 :result-timeout 200  :operation-timeout 1000  :last-event 1400}}
        operations-atom (atom operations)
        reaper-fn (create-reaper-fn operations-atom (fixed-clock checked-at))]

    (reaper-fn)

    ;task1 has exceeded the last result timeout
    ;task2 has exceeded the total operation timeout
    ;task3 has not timed out and is still in progress
    ;task4 has completed normally
    (let [survived @operations-atom]
      (is (= [task3] (vec (map (fn [r] @r) (keys survived)))))
      (is (.isCancelled task1))
      (is (.isCancelled task2)))))

(deftest connect-piped-input-stream-test
  (let [msg "Hello world!"
        charset (Charset/forName "UTF-8")
        msg-bytes (.getBytes msg charset)
        dest-buf (byte-array (count msg-bytes))
        write-fn (fn [os] (.write os msg-bytes))
        [op-fn input-stream] (connect-piped-output-stream write-fn)
        f (future-call op-fn)]
    (.read input-stream dest-buf)
    (.get f 500 TimeUnit/MILLISECONDS)
    (.close input-stream)

    (is (= (seq msg-bytes) (seq dest-buf)))))
