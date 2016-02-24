(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as restapi]
            [drafter.backend.protocols :refer :all]
            [drafter.util :as util]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job complete-job! create-child-job]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.util :as util]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [query]]
            [environ.core :refer [env]]
            [clojure.string :as string]))

;; Note if we change this default value we should also change it in the
;; drafter-client, and possibly other places too.
(def batched-write-size (Integer/parseInt (get env :drafter-batched-write-size "10000")))

(defmacro with-job-exception-handling [job & forms]
  `(try
     ~@forms
     (catch clojure.lang.ExceptionInfo exi#
       (log/warn exi# "Error whilst executing job")
       (complete-job! ~job {:type :error
                            :error-type (str (class exi#))
                            :exception (.getCause exi#)}))
     (catch Exception ex#
       (log/warn ex# "Error whilst executing job")
       (complete-job! ~job {:type :error
                            :error-type (str (class ex#))
                            :exception ex#}))))

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

(defn job-succeeded!
  "Adds the job to the set of finished-jobs as a successfully completed job."
  ([job] (job-succeeded! job {}))
  ([job details] (complete-job! job (merge {:type :ok} details))))

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

(defn delete-in-batches [repo graph contents-only? job]
  ;; Loops until the graph is empty, then deletes state graph if not a
  ;; contents-only? deletion.
  ;;
  ;; Checks that graph is a draft graph - will only delete drafts.
  (if (and (mgmt/graph-exists? repo graph)
           (mgmt/draft-exists? repo graph))
      (do
        (mgmt/delete-graph-batched! repo graph batched-write-size)

        (if (mgmt/graph-exists? repo graph)
          ;; There's more graph contents so queue another job to continue the
          ;; deletions.
          (let [apply-next-batch (partial delete-in-batches repo graph contents-only?)]
            (queue-job! (create-child-job job apply-next-batch)))
          (finish-delete-job! repo graph contents-only? job)))
      (finish-delete-job! repo graph contents-only? job)))

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
    (update! repo query)))

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
   (let [m (first (query repo (graph-count-query graph-uri)))
         graph-count (Integer/parseInt (.stringValue (get m "c")))]
     (calculate-offsets graph-count batch-size))))

;;update-graph-metadata :: Repository -> [URI] -> Seq [String, String] -> Job -> ()
(defn- update-graph-metadata
  "Updates or creates each of the the given graph metadata pairs for
  each given graph under a job."
  [backend graphs metadata job]
  (with-job-exception-handling job
    (append-metadata-to-graphs! backend graphs metadata)
    (complete-job! job restapi/ok-response)))

(defn create-update-metadata-job
  "Creates a job to associate the given graph metadata pairs with each
  given graph."
  [backend graphs metadata]
  (create-job :sync-write (partial update-graph-metadata backend graphs metadata)))

(defn- sparql-uri-list [uris]
  (string/join " " (map #(str "<" % ">") uris)))

(defn- delete-graph-metadata-query [graph-uris meta-uris]
  (str
   "DELETE {"
   (mgmt/with-state-graph "?g ?meta ?o")
   "} WHERE {"
   (mgmt/with-state-graph
     (str
      "?g ?meta ?o
       VALUES ?g {" (sparql-uri-list graph-uris) "}
       VALUES ?meta {" (sparql-uri-list meta-uris) "}"))
   "}"))

(defn- delete-graph-metadata
  "Deletes all metadata values associated with the given keys across
  all the given graph URIs."
  [repo graphs meta-keys job]
  (with-job-exception-handling job
    (let [meta-uris (map meta-uri meta-keys)
          delete-query (delete-graph-metadata-query graphs meta-uris)]
      (update! repo delete-query)
      (complete-job! job restapi/ok-response))))

(defn create-delete-metadata-job
  "Create a job to delete the given metadata keys from a collection of
  draft graphs."
  [repo graphs meta-keys]
  (create-job :sync-write (partial delete-graph-metadata repo graphs meta-keys)))


(defn copy-from-live-graph [repo live-graph-uri dest-graph-uri batches job]
  (with-job-exception-handling job
    (if-let [[offset limit] (first batches)]
      (let [next-fn (fn [job]
                      (copy-from-live-graph repo live-graph-uri dest-graph-uri (rest batches) job))]
        (copy-graph-batch! repo live-graph-uri dest-graph-uri offset limit)
        (queue-job! (create-child-job job next-fn)))
      (job-succeeded! job))))

(defn create-copy-from-live-graph-job [repo draft-graph-uri]
    (make-job :batch-write [job]
              (let [live-graph-uri (mgmt/lookup-live-graph repo draft-graph-uri)
                    batch-sizes (and live-graph-uri
                                     (get-graph-clone-batches repo live-graph-uri))]
                (copy-from-live-graph repo live-graph-uri draft-graph-uri batch-sizes job)
                (job-succeeded! job))))
