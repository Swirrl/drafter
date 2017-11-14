(ns drafter.rdf.draftset-management.jobs
  (:require [clojure.string :as string]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.endpoints :as ep]
            [drafter.rdf.draftset-management.operations :as ops]
            [drafter.draftset :as ds]
            [drafter.rdf.draft-management :as mgmt :refer [to-quads with-state-graph]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-statement]]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.rdf.sparql :as sparql]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter.rdf :as rdf :refer [context]]
            [grafter.rdf.protocols :refer [map->Quad map->Triple]]
            [grafter.rdf4j.formats :as formats]
            [grafter.rdf4j.io :refer [quad->backend-quad rdf-writer]]
            [grafter.rdf4j.repository :as repo]
            [grafter.url :as url]
            [grafter.vocabularies.rdf :refer :all]
            [swirrl-server.async.jobs :as ajobs]
            [clojure.string :as string]
            [drafter.backend.protocols :refer :all]
            [drafter.draftset :as ds]
            [drafter.rdf.draft-management :as mgmt :refer [to-quads with-state-graph]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-statement]]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.rdf.sparql :as sparql]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter.rdf :as rdf :refer [context]]
            [grafter.rdf.protocols :refer [map->Quad map->Triple]]
            [grafter.rdf4j.formats :as formats]
            [grafter.rdf4j.io :refer [quad->backend-quad rdf-writer]]
            [grafter.rdf4j.repository :as repo]
            [grafter.url :as url]
            [grafter.vocabularies.rdf :refer :all]
            [swirrl-server.async.jobs :as ajobs])
  (:import java.io.StringWriter
           [java.util Date UUID]
           org.eclipse.rdf4j.model.impl.ContextStatementImpl
           org.eclipse.rdf4j.model.Resource
           [org.eclipse.rdf4j.query GraphQuery TupleQueryResultHandler]
           org.eclipse.rdf4j.queryrender.RenderUtils))

(defn delete-draftset-job [backend draftset-ref]
  (jobs/make-job
    :background-write [job]
    (do (ops/delete-draftset! backend draftset-ref)
        (jobs/job-succeeded! job))))

(defn- delete-quads-from-draftset [backend quad-batches draftset-ref live->draft {:keys [op job-started-at] :as state} job]
  (case op
    :delete
    (if-let [batch (first quad-batches)]
      (let [live-graph (context (first batch))]
        (if (mgmt/is-graph-managed? backend live-graph)
          (if-let [draft-graph-uri (get live->draft live-graph)]
            (do
              (with-open [conn (repo/->connection (->sesame-repo backend))]
                (mgmt/set-modifed-at-on-draft-graph! conn draft-graph-uri job-started-at)
                (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
                      sesame-statements (map quad->backend-quad rewritten-statements)
                      graph-array (into-array Resource (map util/uri->sesame-uri (vals live->draft)))]
                  (.remove conn sesame-statements graph-array)))
              (let [next-job (ajobs/create-child-job
                              job
                              (partial delete-quads-from-draftset backend (rest quad-batches) draftset-ref live->draft state))]
                (writes/queue-job! next-job)))
            ;;NOTE: Do this immediately as we haven't done any real work yet
            (recur backend quad-batches draftset-ref live->draft (merge state {:op :copy-graph :live-graph live-graph}) job))
          ;;live graph does not exist so do not create a draft graph
          ;;NOTE: This is the same behaviour as deleting a live graph
          ;;which does not exist in live
          (recur backend (rest quad-batches) draftset-ref live->draft state job)))
      (let [draftset-info (ops/get-draftset-info backend draftset-ref)]
        (jobs/job-succeeded! job {:draftset draftset-info})))

    :copy-graph
    (let [{:keys [live-graph]} state
          ds-uri (str (ds/->draftset-uri draftset-ref))
          {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend live-graph live->draft ds-uri)]

      (ops/lock-writes-and-copy-graph backend live-graph draft-graph-uri {:silent true})
      ;; Now resume appending the batch
      (recur backend
             quad-batches
             draftset-ref
             (assoc live->draft live-graph draft-graph-uri)
             (merge state {:op :delete})
             job))))

(defn batch-and-delete-quads-from-draftset [backend quads draftset-ref live->draft job]
  (let [quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (java.util.Date.)]
    (delete-quads-from-draftset backend quad-batches draftset-ref live->draft {:op :delete :job-started-at now} job)))



(defn delete-quads-from-draftset-job [backend draftset-ref serialised rdf-format]
  (jobs/make-job :background-write [job]
                 (let [backend (ep/draftset-endpoint {:backend backend :draftset-ref draftset-ref :union-with-live? false})
                       quads (read-statements serialised rdf-format)
                       graph-mapping (ops/get-draftset-graph-mapping backend draftset-ref)]
                   (batch-and-delete-quads-from-draftset backend quads draftset-ref graph-mapping job))))

(defn delete-triples-from-draftset-job [backend draftset-ref graph serialised rdf-format]
  (jobs/make-job :background-write [job]
                 (let [backend (ep/draftset-endpoint {:backend backend :draftset-ref draftset-ref :union-with-live? false})
                       triples (read-statements serialised rdf-format)
                       quads (map #(util/make-quad-statement % graph) triples)
                       graph-mapping (ops/get-draftset-graph-mapping backend draftset-ref)]
                   (batch-and-delete-quads-from-draftset backend quads draftset-ref graph-mapping job))))

(defn copy-live-graph-into-draftset-job [backend draftset-ref live-graph]
  (jobs/make-job :background-write [job]
                 (let [draft-graph-uri (ops/create-or-empty-draft-graph-for backend draftset-ref live-graph)]
                   (ops/lock-writes-and-copy-graph backend live-graph draft-graph-uri {:silent true})
                   (jobs/job-succeeded! job))))

(defn publish-draftset-job
  "Return a job that publishes the graphs in a draftset to live and
  then deletes the draftset."
  [backend draftset-ref]
  ;; TODO combine these into a single job as priorities have now
  ;; changed how these will be applied.

  (jobs/make-job :publish-write [job]
                 (try
                   (ops/publish-draftset-graphs! backend draftset-ref)
                   (ops/delete-draftset-statements! backend draftset-ref)
                   (jobs/job-succeeded! job)
                   (catch Exception ex
                     (jobs/job-failed! job ex)))))

(declare append-draftset-quads)

(defn- append-draftset-quads*
  [quad-batches live->draft backend job-started-at job draftset-ref state]
  (if-let [batch (first quad-batches)]
    (let [{:keys [graph-uri triples]} (ops/quad-batch->graph-triples batch)]
      (if-let [draft-graph-uri (get live->draft graph-uri)]
        (do
          (ops/append-data-batch! backend draft-graph-uri triples)
          (mgmt/set-modifed-at-on-draft-graph! backend draft-graph-uri job-started-at)

          (let [next-job (ajobs/create-child-job
                          job
                          (partial append-draftset-quads backend draftset-ref live->draft (rest quad-batches) (merge state {:op :append})))]
            (writes/queue-job! next-job)))
        ;;NOTE: do this immediately instead of scheduling a
        ;;continuation since we haven't done any real work yet
        (append-draftset-quads backend draftset-ref live->draft quad-batches (merge state {:op :copy-graph :graph graph-uri}) job)))
    (jobs/job-succeeded! job)))


(defn- copy-graph-for-append*
  [state draftset-ref backend live->draft quad-batches job]
  (let [live-graph-uri (:graph state)
        ds-uri (str (ds/->draftset-uri draftset-ref))
        {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend live-graph-uri live->draft ds-uri)]

    (ops/lock-writes-and-copy-graph backend live-graph-uri draft-graph-uri {:silent true})
    ;; Now resume appending the batch
    (append-draftset-quads backend draftset-ref graph-map quad-batches (merge state {:op :append}) job)))


(defn- append-draftset-quads [backend draftset-ref live->draft quad-batches {:keys [op job-started-at] :as state} job]
  (case op
    :append
    (append-draftset-quads* quad-batches live->draft backend job-started-at job draftset-ref state)

    :copy-graph
    (copy-graph-for-append* state draftset-ref backend live->draft quad-batches job)))

(defn- append-quads-to-draftset-job [backend draftset-ref quads]
  (ajobs/create-job :background-write
                    (fn [job]
                      (let [graph-map (ops/get-draftset-graph-mapping backend draftset-ref)
                            quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
                            now (java.util.Date.)]
                        (append-draftset-quads backend draftset-ref graph-map quad-batches {:op :append :job-started-at now} job)))))

(defn append-data-to-draftset-job [backend draftset-ref tempfile rdf-format]
  (append-quads-to-draftset-job backend draftset-ref (read-statements tempfile rdf-format)))

(defn append-triples-to-draftset-job [backend draftset-ref tempfile rdf-format graph]
  (let [triples (read-statements tempfile rdf-format)
        quads (map (comp map->Quad #(assoc % :c graph)) triples)]
    (append-quads-to-draftset-job backend draftset-ref quads)))


