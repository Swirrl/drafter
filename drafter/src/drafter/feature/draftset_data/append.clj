(ns drafter.feature.draftset-data.append
  "Appending quads & triples into a draftset."
  (:require
   [clojure.spec.alpha :as s]
   [drafter.async.responses :as async-response]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.draftset :as ds]
   [drafter.feature.draftset-data.common :as ds-data-common]
   [drafter.feature.draftset-data.middleware :as dset-middleware]
   [drafter.middleware :refer [require-rdf-content-type temp-file-body inflate-gzipped]]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.requests :as req]
   [drafter.job-responses :as response]
   [drafter.time :as time]
   [grafter-2.rdf.protocols :as pr]
   [grafter-2.rdf4j.io :as gio]
   [grafter-2.rdf4j.repository :as repo]
   [integrant.core :as ig]))

(defn append-data-batch!
  "Appends a sequence of triples to the given draft graph."
  [repo graph-uri triple-batch draftset-ref]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (when-not (empty? triple-batch)
    ;;WARNING: This assumes the backend is a sesame backend which is
    ;;true for all current backends.
    (with-open [conn (repo/->connection repo)]
      (pr/add conn graph-uri triple-batch)
      (mgmt/rewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset-ref)
                                    :deleted :ignore}))))

(defn- append-to-draft-graph [state {:keys [draftset-ref] :as context} draft-graph-uri triples]
  (let [repo (ds-data-common/get-repo context)]
    (append-data-batch! repo draft-graph-uri triples draftset-ref)
    (ds-data-common/draft-graph-appended state context draft-graph-uri)))

(defn- consume-batch
  "Consumes the current quad batch and returns the new state"
  [state quad-batches]
  (if-let [remaining-batches (next quad-batches)]
    (assoc state :quad-batches remaining-batches)
    (ds-data-common/done-state)))

(defn- append-state [{:keys [quad-batches live->draft] :as state} context]
  (let [repo (ds-data-common/get-repo context)
        batch (first quad-batches)]
    (let [{:keys [graph-uri triples]} (ds-data-common/quad-batch->graph-triples batch)
          draft-graph-uri (get live->draft graph-uri)]
      (cond
        ;;draft graph already exists so append current batch
        (some? draft-graph-uri)
        (do
          (let [state (append-to-draft-graph state context draft-graph-uri triples)]
            (consume-batch state quad-batches)))

        ;;live graph exists so clone it and update the draft graph mapping
        (mgmt/is-graph-live? repo graph-uri)
        (let [draft-graph-uri (ds-data-common/copy-user-graph context graph-uri)]
          (ds-data-common/add-draft-graph state graph-uri draft-graph-uri))

        ;;no draft exists or live graph exists so create the managed graph and draft graph in
        ;;draftset before appending current batch
        :else
        (let [draft-graph-uri (ds-data-common/create-user-graph-draft context graph-uri)
              state (ds-data-common/add-draft-graph state graph-uri draft-graph-uri)
              state (append-to-draft-graph state context draft-graph-uri triples)]
          (consume-batch state quad-batches))))))

(defn- start-state [{:keys [quad-batches] :as state} context]
  (if (seq quad-batches)
    (append-state (ds-data-common/move-to state ::append) context)
    (ds-data-common/done-state)))

(defn- validate-non-blank-node
  "Returns quad if its graph component is not a blank node otherwise raises
   an exception"
  [quad]
  (let [g (pr/context quad)]
    (if (pr/blank-node? g)
      (throw (ex-info "Blank node as graph ID" {:type :error}))
      quad)))

(defn validate-graph-source
  "Wraps an ITripleReadable source with one that validates each graph in the
   returned sequence is not a blank node."
  [inner]
  (reify pr/ITripleReadable
    (to-statements [_this _opts]
      (map validate-non-blank-node (gio/statements inner)))))

(defn append-state-machine
  "Creates a state machine for append data jobs"
  []
  (reify ds-data-common/StateMachine
    (create-initial-state [_this live->draft source]
      (let [validating-source (validate-graph-source source)]
        (ds-data-common/init-state ::start live->draft validating-source)))
    (step [_this state context]
      (case (ds-data-common/state-label state)
        ::start (start-state state context)
        ::append (append-state state context)
        (ds-data-common/unknown-label state)))))

(defn append-data-to-draftset-job
  "Creates a job to append quads from a source into a draft on behalf of the given user"
  [{:keys [backend] :as manager} user-id draftset-id source metadata]
  (let [job-meta (jobs/job-metadata backend draftset-id 'append-data-to-draftset metadata)
        sm (append-state-machine)]
    (ds-data-common/create-state-machine-job manager user-id draftset-id source job-meta sm)))

(defn append-data
  "Enqueues a job to append quads from a data source into draftset on behalf of the given user"
  [manager user-id draftset source metadata]
  (let [job (append-data-to-draftset-job manager user-id draftset source metadata)]
    (response/enqueue-async-job! job)))

(defn data-handler
  "Ring handler to append data into a draftset."
  [{:keys [:drafter/manager
           ::time/clock wrap-as-draftset-owner]}]
  (wrap-as-draftset-owner
    (require-rdf-content-type
      (dset-middleware/parse-graph-for-triples
        (temp-file-body
          (inflate-gzipped
            (fn [{:keys [params] :as request}]
              (let [user-id (req/user-id request)
                    {:keys [draftset-id metadata]} params
                    source (ds-data-common/get-request-statement-source request)
                    append-job (append-data manager user-id draftset-id source metadata)]
                (async-response/submitted-job-response append-job)))))))))

(defmethod ig/pre-init-spec ::data-handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::data-handler [_ opts]
  (data-handler opts))
