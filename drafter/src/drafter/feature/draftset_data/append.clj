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
  [quad-batches live->draft resources job-started-at job draftset-ref state]
  (let [repo (-> resources :backend :repo)]
    (if-let [batch (first quad-batches)]
      (let [{:keys [graph-uri triples]} (ds-data-common/quad-batch->graph-triples batch)]
        (if-let [draft-graph-uri (get live->draft graph-uri)]
          (do
            (append-data-batch! repo draft-graph-uri triples)
            (ds-data-common/touch-graph-in-draftset! repo draftset-ref draft-graph-uri job-started-at)

            (let [next-job (ajobs/create-child-job
                            job
                            (partial append-draftset-quads resources draftset-ref live->draft (rest quad-batches) (merge state {:op :append})))]
              (writes/queue-job! next-job)))
          ;;NOTE: do this immediately instead of scheduling a
          ;;continuation since we haven't done any real work yet
          (append-draftset-quads resources draftset-ref live->draft quad-batches (merge state {:op :copy-graph :graph graph-uri}) job)))
      (ajobs/job-succeeded! job))))


(defn- copy-graph-for-append*
  [state draftset-ref resources live->draft quad-batches job]
  (let [live-graph-uri (:graph state)
        ds-uri (str (ds/->draftset-uri draftset-ref))
        repo (-> resources :backend :repo)
        {:keys [draft-graph-uri graph-map]}
        (mgmt/ensure-draft-exists-for repo live-graph-uri live->draft ds-uri)]

    (ds-data-common/lock-writes-and-copy-graph resources live-graph-uri draft-graph-uri {:silent true})
    ;; Now resume appending the batch
    (append-draftset-quads resources draftset-ref graph-map quad-batches (merge state {:op :append}) job)))

(defn- append-draftset-quads
  [resources draftset-ref live->draft quad-batches state job]
  (let [{:keys [op job-started-at]} state]
    (case op
      :append
      (append-draftset-quads* quad-batches live->draft resources job-started-at job draftset-ref state)

      :copy-graph
      (copy-graph-for-append* state draftset-ref resources live->draft quad-batches job))))

(defn- append-quads-to-draftset-job
  [{:keys [backend] :as resources} user-id operation {:keys [draftset-id metadata]} quads clock-fn]
  (let [ds-id (ds/->draftset-id draftset-id)
        meta (jobs/job-metadata backend ds-id operation metadata)]
    (jobs/make-job backend user-id meta ds-id :background-write
      (fn [job]
        (let [graph-map (ops/get-draftset-graph-mapping backend draftset-id)
              quad-batches (util/batch-partition-by quads
                                                    pr/context
                                                    jobs/batched-write-size)
              now (clock-fn)]
          (append-draftset-quads resources
                                 draftset-id
                                 graph-map
                                 quad-batches
                                 {:op :append :job-started-at now}
                                 job))))))

(defn append-data-to-draftset-job
  [resources user-id tempfile {:keys [ rdf-format] :as params} clock-fn]
  (append-quads-to-draftset-job resources
                                user-id
                                'append-quads-to-draftset
                                params
                                (dses/read-statements tempfile rdf-format)
                                clock-fn))

(defn append-triples-to-draftset-job
  [resources user-id tempfile {:keys [rdf-format graph] :as params} clock-fn]
  (let [triples (dses/read-statements tempfile rdf-format)
        quads (map (comp pr/map->Quad #(assoc % :c graph)) triples)]
    (append-quads-to-draftset-job resources
                                  user-id
                                  'append-triples-to-draftset
                                  params
                                  quads
                                  clock-fn)))

(defn data-handler
  "Ring handler to append data into a draftset."
  [{:keys [:drafter/backend :drafter/global-writes-lock wrap-as-draftset-owner]}]
  (let [resources {:backend backend :global-writes-lock global-writes-lock}]
    (wrap-as-draftset-owner
     (require-rdf-content-type
      (dset-middleware/require-graph-for-triples-rdf-format
       (temp-file-body
        (fn [{:keys [params body] :as request}]
          (let [user-id (req/user-id request)]
            (if (is-quads-format? (:rdf-format params))
              (let [append-job (append-data-to-draftset-job resources
                                                            user-id
                                                            body
                                                            params
                                                            util/get-current-time)]
                (response/submit-async-job! append-job))
              (let [append-job (append-triples-to-draftset-job resources
                                                               user-id
                                                               body
                                                               params
                                                               util/get-current-time)]
                (response/submit-async-job! append-job)))))))))))

(defmethod ig/pre-init-spec ::data-handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::data-handler [_ opts]
  (data-handler opts))
