(ns drafter.feature.read-only-mode-test
  (:require
    [clojure.test :refer :all :as t]
    [drafter.user-test :refer [test-publisher]]
    [drafter.feature.draftset.test-helper :as dh]
    [grafter-2.rdf4j.io :refer [statements]]
    [drafter.test-common :as tc]
    [drafter.async.jobs :refer [->Job create-job job-succeeded!]]
    [drafter.write-scheduler :as scheduler])
  (:import (clojure.lang ExceptionInfo)))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")

(def mock-user-id "dummy@user.com")

(defn job-with-timeout-task
  "Job which simulates some blocking work being done with a timeout"
  [priority ret]
  (create-job mock-user-id
              {:operation 'test-job}
              priority
              (fn [job]
                (tc/timeout 400 #(Thread/sleep 300))
                (job-succeeded! job ret))))

(defn- count-realised-jobs [jobs]
  (->> (map :value-p jobs)
       (filter realized?)
       (count)))

(tc/deftest-system-with-keys reject-mode-test
   [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
   [system system-config]
   (let [handler (get system [:drafter/routes :draftset/api])
         quads (statements "test/resources/test-draftset.trig")
         draftset-location (dh/create-draftset-through-api handler test-publisher)]
     (try
       (tc/timeout 200 #(scheduler/toggle-reject-and-flush!))

       (testing "append requests are rejected when reject mode is active"
         (let [append-request (dh/statements->append-request test-publisher draftset-location quads {:format :nq})]
           (is
             (thrown-with-msg?
               ExceptionInfo
               #"Write operations are temporarily unavailable due to maintenance"
               (tc/assert-is-service-unavailable-response (handler append-request))))))

       ;(tc/timeout 1200 #(Thread/sleep 1000))

       (finally
         (tc/timeout 200 #(scheduler/toggle-reject-and-flush!))))))
