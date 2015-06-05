(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as restapi]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job complete-job!]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.util :as util]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [query with-transaction ToConnection ->connection]]
            [environ.core :refer [env]]))

;; Note if we change this default value we should also change it in the
;; drafter-client, and possibly other places too.
(def batched-write-size (Integer/parseInt (get env :drafter-batched-write-size "10000")))

(defmacro with-job-exception-handling [job & forms]
  `(try
     ~@forms
     (catch clojure.lang.ExceptionInfo exi#
       (complete-job! ~job {:type :error
                            :error-type (str (class exi#))
                            :exception (.getCause exi#)}))
     (catch Exception ex#
       (complete-job! ~job {:type :error
                            :error-type (str (class ex#))
                            :exception ex#}))))

(defn failed-job-result?
  "Indicates whether the given result object is a failed job result."
  [{:keys [type] :as result}]
  (= :error type))

(defmacro make-job [write-priority [job :as args] & forms]
  `(create-job ~write-priority
               (fn [~job]
                 (with-job-exception-handling ~job
                   ~@forms))))

(defn- job-succeeded!
  "Adds the job to the set of finished-jobs as a successfully completed job."
  ([job] (job-succeeded! job {}))
  ([job details] (complete-job! job (merge {:type :ok} details))))

(defn create-draft-job [repo live-graph params]
  (make-job :sync-write [job]
            (let [conn (->connection repo)
                  draft-graph-uri (with-transaction conn
                                    (mgmt/create-managed-graph! conn live-graph)
                                    (mgmt/create-draft-graph! conn
                                                              live-graph (meta-params params)))]

              (job-succeeded! job {:guri draft-graph-uri}))))

(defn- finish-delete-job! [repo graph contents-only? job]
  (when-not contents-only?
    (mgmt/delete-draft-graph-and-its-state! repo graph))
  (job-succeeded! job))

(defn- delete-in-batches [repo graph contents-only? job]
  ;; Loops until the graph is empty, then deletes state graph if not a
  ;; contents-only? deletion.
  ;;
  ;; Checks that graph is a draft graph - will only delete drafts.
  (let [conn (->connection repo)]
    (if (and (mgmt/graph-exists? repo graph)
             (mgmt/draft-exists? repo graph))
      (do
        (with-transaction conn
                          (mgmt/delete-graph-batched! conn graph batched-write-size))

        (if (mgmt/graph-exists? repo graph)
          ;; There's more graph contents so queue another job to continue the
          ;; deletions.
          (let [apply-next-batch (partial delete-in-batches repo graph contents-only?)]
            (queue-job! (assoc job :function apply-next-batch)))
          (finish-delete-job! repo graph contents-only? job)))
      (finish-delete-job! repo graph contents-only? job))))

(defn delete-graph-job [repo graph & {:keys [contents-only?]}]
  "Deletes graph contents as per batch size in order to avoid blocking
   writes with a lock. Finally the graph itself will be deleted unless
   a value is supplied for the :contents-only? keyword argument"
  (log/info "Starting batch deletion job")
  (create-job :batch-write
              (partial delete-in-batches
                       repo
                       graph
                       contents-only?)))

(defn- append-data-in-batches [repo draft-graph metadata triples job]
  (with-job-exception-handling job
    (let [conn (->connection repo)
          [current-batch remaining-triples] (split-at batched-write-size triples)]

      (log/info (str "Adding a batch of triples to repo" current-batch))
      (with-transaction conn
        (mgmt/append-data! conn draft-graph current-batch))

      (if-not (empty? remaining-triples)
        ;; resubmit the remaining batches under the same job to the
        ;; queue to give higher priority jobs a chance to write
        (let [apply-next-batch (partial append-data-in-batches
                                        repo draft-graph metadata remaining-triples)]
          (queue-job! (assoc job :function apply-next-batch)))

        (do
          (mgmt/add-metadata-to-graph conn draft-graph metadata)
          (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))

          (job-succeeded! job))))))

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

(defn update-job-fn
  "Updates the function associated with a job. This is used to create
  a continuation job to be queued."
  [job f]
  (assoc job :function f))

(defn clone-graph-and-append-in-batches
  "Clones a source graph and then appends the given metadata and
  statements to it in batches."
  [repo source-graph dest-graph batches metadata new-triples job]
  (with-job-exception-handling job
    (queue-job!
     (update-job-fn job
                    (if-let [[offset limit] (first batches)]
                      (let [next-fn (fn [job]
                                      (clone-graph-and-append-in-batches repo source-graph dest-graph (rest batches) metadata new-triples job))]
                        (copy-graph-batch! repo source-graph dest-graph offset limit)
                        next-fn)

                      ;; else if there are no (more) batches start the appending
                      ;; the file to the graph.
                      (partial append-data-in-batches repo dest-graph metadata new-triples))))))

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
  [repo graphs metadata job]
  (with-job-exception-handling job
    (with-open [conn (->connection repo)]
      (doseq [draft-graph graphs]
        (mgmt/add-metadata-to-graph conn draft-graph metadata))
      (complete-job! job restapi/ok-response))))

(defn create-update-metadata-job
  "Creates a job to associate the given graph metadata pairs with each
  given graph."
  [repo graphs metadata]
  (create-job :sync-write (partial update-graph-metadata repo graphs metadata)))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes.

  It works by concatenating the existing live quads with a lazy-seq on
  the uploaded file.  This combined lazy sequence is then split into
  the current batch and remaining, with the current batch being
  applied before the job is resubmitted (under the same ID) with the
  remaining triples.

  The last batch is finally responsible for signaling job completion
  via a side-effecting call to complete-job!"

  [repo draft-graph tempfile rdf-format metadata]

  (let [new-triples (statements tempfile
                                :format rdf-format
                                :buffer-size batched-write-size)

        ;; NOTE that this is technically not transactionally safe as
        ;; sesame currently only supports the READ_COMMITTED isolation
        ;; level.
        ;;
        ;; As there is no read lock or support for (repeatable reads)
        ;; this means that the CONSTRUCT below can witness data
        ;; changing underneath it.
        ;;
        ;; TODO: protect against this, either by adopting a better
        ;; storage engine or by adding code to either refuse make-live
        ;; operations on jobs that touch the same graphs that we're
        ;; manipulating here, or to raise an error on the batch task.
        ;;
        ;; I think the newer versions of Sesame 1.8.x might also provide better
        ;; support for different isolation levels, so we might want to consider
        ;; upgrading.
        ;;
        ;; http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Read_committed
        ;;
        ;; This can occur if a user does a make-live on a graph
        ;; which is being written to in a batch job.
    ]

    (make-job :batch-write [job]
              ;;copy the live graph (if it exists) and then append the file data
              (queue-job! (update-job-fn job
                                         (let [live-graph-uri (mgmt/lookup-live-graph repo draft-graph)
                                               batch-sizes (and live-graph-uri
                                                                (get-graph-clone-batches repo live-graph-uri))]
                                           (fn [job]
                                             (clone-graph-and-append-in-batches repo live-graph-uri draft-graph batch-sizes metadata new-triples job))))))))

(defn migrate-graph-live-job [repo graph]
  (make-job :exclusive-write [job]
            (log/info "Starting make-live for graph" graph)
            (let [conn (->connection repo)]
              (with-transaction conn
                (if (instance? String graph)
                  (mgmt/migrate-live! conn graph)
                  (doseq [g graph]
                    (mgmt/migrate-live! conn g)))))
            (log/info "Make-live for graph" graph "done")
            (job-succeeded! job)))
