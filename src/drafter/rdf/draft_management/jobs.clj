(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer :all]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job job-succeeded! job-failed! create-child-job]]
            [swirrl-server.errors :refer [encode-error]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [environ.core :refer [env]]
            [drafter.util :as util]))

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

(def batched-write-size (Integer/parseInt (get env :drafter-batched-write-size "75000")))

(defmacro with-job-exception-handling [job & forms]
  `(try
     ~@forms
     (catch Throwable ext#
       (log/warn ext# "Error whilst executing job")
       (job-failed! ~job ext#))))

(defn failed-job-result?
  "Indicates whether the given result object is a failed job result."
  [{:keys [type] :as result}]
  (= :error type))

(defmacro make-job [write-priority [job :as args] & forms]
  `(create-job ~write-priority
               (fn [~job]
                 (with-job-exception-handling ~job
                   ~@forms))))

(defmacro action-joblet
  "Creates a joblet function which ignores its state parameter and
  returns nil."
  [& body]
  `(fn [_#]
     ~@body
     nil))

(defn exec-joblets
  "Executes a sequence of joblets under the given job. A joblet is a
  function (State -> State) which should also have some side-effect
  which represents a batch of work the larger job should execute. The
  State parameter is available for joblets to communicate some extra
  information to the next joblet. The first joblet is executed
  immediately and each subsequent job is re-scheduled for later
  execution. After all joblets have been executed the job is completed
  with the result of the last joblet."
  ([joblet-seq job] (exec-joblets joblet-seq nil job))
  ([joblet-seq state job]
   (with-job-exception-handling job
     (if-let [joblet (first joblet-seq)]
       (let [next-state (joblet state)
             next-job (partial exec-joblets (rest joblet-seq) next-state)]
         (queue-job! (create-child-job job next-job)))
       (job-succeeded! job state)))))

(defn joblet-seq->job
  "Creates a new job from a sequence of joblets. If provided the state
  is passed to the first joblet on execution."
  ([joblet-seq] (joblet-seq->job joblet-seq :batch-write nil))
  ([joblet-seq write-priority] (joblet-seq->job joblet-seq write-priority nil))
  ([joblet-seq write-priority state]
   (make-job write-priority [job]
                  (exec-joblets joblet-seq state job))))

(defn copy-graph-batch-query
  "Query to copy a range of data in a source graph into a destination
  graph."
  [source-graph dest-graph offset limit]
  (str "INSERT {
          GRAPH <" dest-graph "> {
            ?s ?p ?o
          }
        } WHERE {
          SELECT ?s ?p ?o WHERE {
            GRAPH <" source-graph "> {
              ?s ?p ?o
            }
          } LIMIT " limit " OFFSET " offset "
       }"))

(defn copy-graph-batch!
  "Copies the data segmented by offset and limit in the source graph
  into the given destination graph."
  [repo source-graph dest-graph offset limit]
  (let [query (copy-graph-batch-query source-graph dest-graph offset limit)]
    (mgmt/update! repo query)))

(defn calculate-offsets [count batch-size]
  "Given a total number of items and a batch size, returns a sequence
  of [offset limit] pairs for segmenting the source collection into
  batches.

  The limit will always be set to batch-size."
  (take (/ count batch-size)
        (iterate (fn [[offset batch-size]]
                   [(+ offset batch-size) batch-size])
                 [0 batch-size])))

(defn graph-count-query [graph]
  (str "SELECT (COUNT(*) as ?c) WHERE {
          GRAPH <" graph "> { ?s ?p ?o }
        }"))

(defn get-graph-clone-batches
  ([repo graph-uri] (get-graph-clone-batches repo graph-uri batched-write-size))
  ([repo graph-uri batch-size]
   (let [m (first (util/query-eager-seq repo (graph-count-query graph-uri)))
         graph-count (Integer/parseInt (.stringValue (get m "c")))]
     (calculate-offsets graph-count batch-size))))

(defn copy-from-live-graph [repo live-graph-uri dest-graph-uri batches job]
  (with-job-exception-handling job
    (if-let [[offset limit] (first batches)]
      (let [next-fn (fn [job]
                      (copy-from-live-graph repo live-graph-uri dest-graph-uri (rest batches) job))]
        (copy-graph-batch! repo live-graph-uri dest-graph-uri offset limit)
        (log/info "There are" (count batches) "remaining batches")
        (queue-job! (create-child-job job next-fn)))
      (job-succeeded! job))))
