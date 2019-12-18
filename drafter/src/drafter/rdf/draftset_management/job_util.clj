(ns drafter.rdf.draftset-management.job-util
  (:require [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [clojure.string :as str]
            [drafter.util :as util]
            [drafter.async.jobs :as ajobs]
            [drafter.backend.draftset.operations :as dsops]
            [martian.encoders :as enc]))

;; The following times were taken on stardog 4.1.2, in order to determine a better
;; batched write size.  The tests were performed with the dataset:
;;
;; http://statistics.gov.scot/graph/employment-support-allowance
;;
;; This dataset contains 12,268972 quads
;;
;; Running on an AWS m4.2xlarge instance running with JVM args:
;;
;; -Xms5g -Xmx5g -XX:MaxDirectMemorySize=8g
;;
;; The operation was appending a single triple into a graph, but the times are
;; testing the copy from live that we do as part of our Copy On Write behaviour.
;;
;;
;; |      batch size k | # batches | 1st qu btime (ms) | median btime (ms) | 3rd qu btime (ms) | upper qu (95%) | total time | tput (quads/ps) |
;; |-------------------+-----------+-------------------+-------------------+-------------------+----------------+------------+-----------------|
;; | N/A (SPARQL COPY) |         1 |               N/A |               N/A |               N/A |            N/A | 2m 20s     |           84034 |
;; |                10 |      1227 |              2982 |              3388 |              3887 |           4221 | 49m 52s    |            4100 |
;; |                50 |       246 |              2872 |              3211 |              3858 |           4439 | 13m 40s    |           14962 |
;; |                75 |       164 |              2800 |              3357 |              4126 |           4811 | 9m 18s     |           21987 |
;; |               100 |       123 |              4058 |              4454 |              4714 |           5100 | 9m 9s      |           22347 |
;; |               200 |        62 |              5570 |              6730 |              8811 |          10781 | 7m 18s     |           28011 |

(def batched-write-size 75000)

(defn- record-job-stats!
  "Log a job completion to datadog"
  [job suffix]
  (datadog/increment! (util/statsd-name "drafter.job." (:priority job) suffix) 1))

(defn job-failed!
  "Wrap drafter.async.jobs/job-failed! with a datadog counter."
  ([job ex]
   (record-job-stats! job :failed)
   (ajobs/job-failed! job ex))

  ([job ex details]
   (record-job-stats! job :failed)
   (ajobs/job-failed! job ex details)))

(defn job-succeeded!
  "Wrap drafter.async.jobs/job-succeeded! with a datadog counter."
  ([job]
   (record-job-stats! job :succeeded)
   (ajobs/job-succeeded! job))
  ([job details]
   (record-job-stats! job :succeeded)
   (ajobs/job-succeeded! job details)))

(defn init-job-settings!
  "Initialised job settings from the given configuration map."
  [config]
  (when-let [configured-batch-size (:batched-write-size config)]
    (alter-var-root #'batched-write-size (constantly configured-batch-size))))

(defn failed-job-result?
  "Indicates whether the given result object is a failed job result."
  [{:keys [type] :as result}]
  (= :error type))

(defn- get-user-metadata [request-metadata]
  (let [meta (enc/json-decode request-metadata keyword)]
    (when (map? meta) meta)))

(defn job-metadata [backend draftset-id operation request-metadata]
  (let [ds-meta (some->> draftset-id (dsops/get-draftset-info backend))
        user-meta (get-user-metadata request-metadata)]
    (merge {:draftset ds-meta :operation operation} user-meta)))

(defn make-job
  {:style/indent :defn}
  [user-id write-priority metadata job-fn]
  (ajobs/create-job
    user-id
    metadata
    write-priority
    (fn [job]
      (datadog/measure!
        (util/statsd-name "drafter.job" (:operation metadata) write-priority "time")
        {}
        (try
          (job-fn job)
          (catch Throwable t
            (log/warn t "Error whilst executing job")
            (ajobs/job-failed! job t)))))))
