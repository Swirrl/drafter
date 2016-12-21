(ns drafter.rdf.draft-management.jobs-test
  (:require [drafter.rdf.draft-management.jobs :refer :all]
            [clojure.test :refer :all]
            [drafter.test-common :refer [wrap-db-setup]]
            [drafter.write-scheduler :as scheduler]))


(defn succeeded-job-result? [{:keys [type] :as result}]
  (= :ok type))

(deftest exec-joblets-test
  (let [shared (atom [])
        nums (range 1 10)
        joblets (map #(action-joblet (swap! shared conj %)) nums)
        {:keys [value-p] :as job} (joblet-seq->job joblets)]
    (scheduler/queue-job! job)
    (let [result @value-p]
      (is (succeeded-job-result? result))
      (is (= nums @shared)))))

(use-fixtures :each wrap-db-setup)
