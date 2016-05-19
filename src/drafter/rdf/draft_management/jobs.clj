(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as restapi]
            [drafter.backend.protocols :refer :all]
            [drafter.util :as util]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job job-succeeded! job-failed! create-child-job]]
            [swirrl-server.errors :refer [encode-error]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.util :as util]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [environ.core :refer [env]]
            [clojure.string :as string]))

;; Note if we change this default value we should also change it in the
;; drafter-client, and possibly other places too.
(def batched-write-size (Integer/parseInt (get env :drafter-batched-write-size "10000")))

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

(defn succeeded-job-result? [{:keys [type] :as result}]
  (= :ok type))

(defmacro make-job [write-priority [job :as args] & forms]
  `(create-job ~write-priority
               (fn [~job]
                 (with-job-exception-handling ~job
                   ~@forms))))

(defn- finish-delete-job! [repo graph contents-only? job]
  (when-not contents-only?
    (mgmt/delete-draft-graph-state! repo graph))
  (job-succeeded! job))

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
   (let [m (first (mgmt/query repo (graph-count-query graph-uri)))
         graph-count (Integer/parseInt (.stringValue (get m "c")))]
     (calculate-offsets graph-count batch-size))))

(defn copy-from-live-graph [repo live-graph-uri dest-graph-uri batches job]
  (with-job-exception-handling job
    (if-let [[offset limit] (first batches)]
      (let [next-fn (fn [job]
                      (copy-from-live-graph repo live-graph-uri dest-graph-uri (rest batches) job))]
        (copy-graph-batch! repo live-graph-uri dest-graph-uri offset limit)
        (queue-job! (create-child-job job next-fn)))
      (job-succeeded! job))))
