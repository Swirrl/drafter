(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as restapi]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job complete-job!]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
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

  [repo draft-graph {:keys [tempfile size filename content-type] :as file} metadata]

  (let [new-triples (statements tempfile
                                :format (mimetype->rdf-format content-type)
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

        triples (lazy-cat (query repo
                                 (str "CONSTRUCT { ?s ?p ?o } "
                                      "WHERE { "
                                      (mgmt/with-state-graph
                                        "?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                                        "      <" drafter:hasDraft "> <" draft-graph "> .")
                                      "  GRAPH ?live { "
                                      "    ?s ?p ?o . "
                                      "  }"
                                      "}"))

                          new-triples)]

    (create-job :batch-write
                (partial append-data-in-batches
                         repo draft-graph metadata triples))))

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
