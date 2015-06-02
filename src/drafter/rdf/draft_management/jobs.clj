(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as restapi]
            [drafter.util :as util]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job complete-job!]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [->connection query
                                            with-transaction ToConnection]]
            [grafter.rdf.protocols :refer [update!]]
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
        (mgmt/delete-graph-batched! repo graph batched-write-size)

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

      (log/info (str "Adding a batch of triples to repo " current-batch))

      ;; WARNING: Massive hack!  When using the SPARQL repository, only
      ;; operations which directly add or remove statements take part
      ;; in a transaction. Attempting to submit an empty transaction
      ;; causes an exception to be thrown as the accumulated
      ;; transaction string is empty. Any other operations (e.g. direct
      ;; UPDATE operations DO NOT take part in the transaction at all).
      ;; The append-data! function only adds the statements in
      ;; current-batch to the connection and these are therefore the
      ;; only things operating under the transaction. Do not create
      ;; a transaction if there are no statements in the curent batch
      ;; since this will throw an exception on commit.
      (if (empty? current-batch)
        (mgmt/append-data! conn draft-graph current-batch)
        (with-transaction conn
          (mgmt/append-data! conn draft-graph current-batch)))

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
            (let [graphs-to-migrate (if (instance? String graph) [graph] graph)
                  graph-migrate-queries (mapcat #(:queries (mgmt/migrate-live-queries repo %)) graphs-to-migrate)
                  update-str (util/make-compound-sparql-query graph-migrate-queries)]
              (update! repo update-str))
            (log/info "Make-live for graph(s) " graph " done")
            (job-succeeded! job)))
