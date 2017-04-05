(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [drafter.rdf.sparql :as sparql]
            [drafter.write-scheduler :refer [queue-job!]]
            [environ.core :refer [env]]
            [swirrl-server.async.jobs
             :refer
             [create-child-job create-job job-failed! job-succeeded!]]))

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
    (sparql/update! repo query)))

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

;; TODO remove this
(defn get-graph-clone-batches
  ([repo graph-uri] (get-graph-clone-batches repo graph-uri batched-write-size))
  ([repo graph-uri batch-size]
   (let [m (first (sparql/query-eager-seq repo (graph-count-query graph-uri)))
         graph-count (Integer/parseInt (.stringValue (get m "c")))]
     (calculate-offsets graph-count batch-size))))

;; TODO remove this
(defn copy-from-live-graph [repo live-graph-uri dest-graph-uri batches job]
  (with-job-exception-handling job
    (if-let [[offset limit] (first batches)]
      (let [next-fn (fn [job]
                      (copy-from-live-graph repo live-graph-uri dest-graph-uri (rest batches) job))]
        (copy-graph-batch! repo live-graph-uri dest-graph-uri offset limit)
        (log/info "There are" (count batches) "remaining batches")
        (queue-job! (create-child-job job next-fn)))
      (job-succeeded! job))))
