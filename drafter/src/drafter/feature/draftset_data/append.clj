(ns drafter.feature.draftset-data.append
  "Appending quads & triples into a draftset."
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.draftset :as ds]
            [drafter.feature.draftset-data.common :as ds-data-common]
            [drafter.feature.draftset-data.middleware :as dset-middleware]
            [drafter.middleware :refer [require-rdf-content-type temp-file-body]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :as dses :refer [is-quads-format?]]
            [drafter.rdf.sparql :as sparql]
            [drafter.responses :as response]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter-2.rdf.protocols :as pr]
            [integrant.core :as ig]
            [drafter.async.jobs :as ajobs]
            [drafter.requests :as req]))

(defn append-data-batch!
  "Appends a sequence of triples to the given draft graph."
  [conn graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (when-not (empty? triple-batch)
    ;;WARNING: This assumes the backend is a sesame backend which is
    ;;true for all current backends.
    (sparql/add conn graph-uri triple-batch)))

(declare append-draftset-quads)

(defn- append-draftset-quads*
  [quad-batches live->draft backend job-started-at job draftset-ref state]
  (if-let [batch (first quad-batches)]
    (let [{:keys [graph-uri triples]} (ds-data-common/quad-batch->graph-triples batch)]
      (if-let [draft-graph-uri (get live->draft graph-uri)]
        (do
          (append-data-batch! backend draft-graph-uri triples)
          (ds-data-common/touch-graph-in-draftset! backend draftset-ref draft-graph-uri job-started-at)

          (let [next-job (ajobs/create-child-job
                          job
                          (partial append-draftset-quads backend draftset-ref live->draft (rest quad-batches) (merge state {:op :append})))]
            (writes/queue-job! next-job)))
        ;;NOTE: do this immediately instead of scheduling a
        ;;continuation since we haven't done any real work yet
        (append-draftset-quads backend draftset-ref live->draft quad-batches (merge state {:op :copy-graph :graph graph-uri}) job)))
    (ajobs/job-succeeded! job)))


(defn- copy-graph-for-append*
  [state draftset-ref backend live->draft quad-batches job]
  (let [live-graph-uri (:graph state)
        ds-uri (str (ds/->draftset-uri draftset-ref))
        {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend live-graph-uri live->draft ds-uri)]

    (ds-data-common/lock-writes-and-copy-graph backend live-graph-uri draft-graph-uri {:silent true})
    ;; Now resume appending the batch
    (append-draftset-quads backend draftset-ref graph-map quad-batches (merge state {:op :append}) job)))


(defn- append-draftset-quads [backend draftset-ref live->draft quad-batches {:keys [op job-started-at] :as state} job]
  (case op
    :append
    (append-draftset-quads* quad-batches live->draft backend job-started-at job draftset-ref state)

    :copy-graph
    (copy-graph-for-append* state draftset-ref backend live->draft quad-batches job)))

(defn- append-quads-to-draftset-job
  [backend user-id operation draftset-ref quads clock-fn]
  (let [backend (:repo backend)
        ds-id (ds/->draftset-id draftset-ref)]
    (jobs/make-job user-id operation ds-id :background-write
      (fn [job]
        (let [graph-map (ops/get-draftset-graph-mapping backend draftset-ref)
              quad-batches (util/batch-partition-by quads
                                                    pr/context
                                                    jobs/batched-write-size)
              now (clock-fn)]
          (append-draftset-quads backend
                                 draftset-ref
                                 graph-map
                                 quad-batches
                                 {:op :append :job-started-at now}
                                 job))))))

(defn append-data-to-draftset-job
  [backend user-id draftset-ref tempfile rdf-format clock-fn]
  (append-quads-to-draftset-job backend
                                user-id
                                'append-quads-to-draftset
                                draftset-ref
                                (dses/read-statements tempfile rdf-format)
                                clock-fn))

(defn append-triples-to-draftset-job
  [backend user-id draftset-ref tempfile rdf-format graph clock-fn]
  (let [triples (dses/read-statements tempfile rdf-format)
        quads (map (comp pr/map->Quad #(assoc % :c graph)) triples)]
    (append-quads-to-draftset-job backend
                                  user-id
                                  'append-triples-to-draftset
                                  draftset-ref
                                  quads
                                  clock-fn)))

(defn data-handler
  "Ring handler to append data into a draftset."
  [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
  (wrap-as-draftset-owner
   (require-rdf-content-type
    (dset-middleware/require-graph-for-triples-rdf-format
     (temp-file-body
      (fn [{{draftset-id :draftset-id
            rdf-format :rdf-format
            graph :graph} :params body :body :as request}]
        (let [user-id (req/user-id request)
              ds-id (:id draftset-id)]
          (if (is-quads-format? rdf-format)
            (let [append-job (append-data-to-draftset-job backend
                                                          user-id
                                                          ds-id
                                                          body
                                                          rdf-format
                                                          util/get-current-time)]
              (response/submit-async-job! append-job))
            (let [append-job (append-triples-to-draftset-job backend
                                                             user-id
                                                             ds-id
                                                             body
                                                             rdf-format
                                                             graph
                                                             util/get-current-time)]
              (response/submit-async-job! append-job))))))))))

(defmethod ig/pre-init-spec ::data-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::data-handler [_ opts]
  (data-handler opts))
