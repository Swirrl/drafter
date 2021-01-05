(ns drafter.feature.draftset-data.common
  (:require
   [clojure.string :as str]
   [drafter.async.jobs :as ajobs]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.backend.draftset.graphs :as graphs]
   [drafter.backend.draftset.operations :as ops]
   [drafter.draftset :as ds]
   [drafter.rdf.drafter-ontology :refer :all]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.rdf.sesame :as ses]
   [drafter.rdf.sparql :as sparql]
   [drafter.time :as time]
   [drafter.util :as util]
   [drafter.write-scheduler :as writes]
   [grafter-2.rdf.protocols :as pr :refer [context]]
   [grafter-2.rdf4j.io :as gio :refer [rdf-writer]]
   [grafter.vocabularies.dcterms :refer [dcterms:modified]])
  (:import
   java.io.StringWriter))

(defn touch-graph-in-draftset
  "Builds and returns an update string to update the dcterms:modified and
   drafter:version of the supplied resource draft-graph/draftset."
  [draftset-ref draft-graph-uri modified-at]
  (let [version (util/version)]
    (str/join
     " ; "
     [(mgmt/set-timestamp draft-graph-uri dcterms:modified modified-at)
      (mgmt/set-timestamp (ds/->draftset-uri draftset-ref) dcterms:modified modified-at)
      (mgmt/set-version (ds/->draftset-uri draftset-ref) version)])))

(defn touch-graph-in-draftset!
  "Updates the dcterms:modified and drafter:version on the given draftgraph and
   draftset."
  [backend draftset-ref draft-graph-uri modified-at]
  (sparql/update! backend
                  (touch-graph-in-draftset draftset-ref draft-graph-uri modified-at)))

(defn quad-batch->graph-triples
  "Extracts the graph-uri from a sequence of quads and converts all
  quads into triples. Batch must be non-empty and each contained quad
  should have the same graph. If the quads have a nil context an
  exception is thrown as drafts for the default graph are not
  currently supported."
  [quads]
  {:pre [(not (empty? quads))]}
  (let [graph-uri (pr/context (first quads))]
    (if (some? graph-uri)
      {:graph-uri graph-uri :triples (map pr/map->Triple quads)}
      (let [sw (StringWriter.)]
        (pr/add (rdf-writer sw :format :nq) (take 5 quads))
        (throw (IllegalArgumentException.
                (str "All statements must have an explicit target graph. The following statements have no graph:\n" sw)))))))

(defn- source->quad-batches
  "Reads a statement source into a sequence of quad batches"
  [source]
  (let [quads (gio/statements source)]
    (util/batch-partition-by quads pr/context jobs/batched-write-size)))

(defn get-request-statement-source
  "Returns an ITripleReadable statement source from an incoming jobs request"
  [{:keys [body params] :as request}]
  (let [{:keys [rdf-format graph]} params
        source (ses/->FormatStatementSource body rdf-format)]
    (if (ses/is-quads-format? rdf-format)
      source
      (ses/->GraphTripleStatementSource source graph))))

(defn lock-writes-and-copy-graph
  "Calls mgmt/copy-graph to copy a live graph into the draftset, but
  does so with the writes lock engaged.  This allows us to fail
  concurrent sync-writes fast."
  [manager live-graph-uri draft-graph-uri opts]
  (writes/with-lock (:global-writes-lock manager) :copy-graph
    ;; Execute the graph copy inside the write-lock so we can
    ;; fail :blocking-write operations if they are waiting longer than
    ;; their timeout period for us to release it.  These writes would
    ;; likely be blocked inside the database anyway, so this way we
    ;; can fail them fast when they are run behind a long running op.
    (mgmt/copy-graph (:backend manager) live-graph-uri draft-graph-uri opts)))

(defn job-context
  "Creates a static 'context' for an update job. The context contains data that does not change
   throughout the execution of the job (in contrast to the state)."
  [{:keys [clock] :as manager} draftset-ref]
  {:manager manager
   :draftset-ref draftset-ref
   :job-started-at (time/now clock)})

(defn get-repo
  "Fetch the repository from the job context"
  [context]
  (get-in context [:manager :backend]))

(defn- graph-manager [context]
  (get-in context [:manager :graph-manager]))

(defn create-user-graph-draft
  "Creates a new draft for a live graph within the draft of the executing job"
  [{:keys [draftset-ref] :as context} live-graph-uri]
  (graphs/create-user-graph-draft (graph-manager context) draftset-ref live-graph-uri))

(defn copy-user-graph
  "Copies a live graph into a new draft graph within the draft of the executing job"
  [{:keys [manager] :as context} live-graph-uri]
  (let [draft-graph-uri (create-user-graph-draft context live-graph-uri)]
    (lock-writes-and-copy-graph manager live-graph-uri draft-graph-uri {:silent true})
    draft-graph-uri))

(defn get-draft-graph
  "Returns the draft graph URI for the specified live graph in the current state or
   nil if no draft exists"
  [{:keys [live->draft] :as state} live-graph-uri]
  (get live->draft live-graph-uri))

(defn add-draft-graph
  "Adds a live->draft graph mapping to the current state and returns the new state"
  [state live-graph-uri draft-graph-uri]
  (update state :live->draft assoc live-graph-uri draft-graph-uri))

(defn init-state
  "Creates an initial state with the given label, live->draft graph mapping and
   statement source"
  [label live->draft source]
  (let [quad-batches (source->quad-batches source)]
    {:label label :live->draft live->draft :quad-batches quad-batches}))

(defn done-state
  "Returns a state indicating the job has completed"
  ([] {:label ::done})
  ([result] {:label ::done ::result result}))

(defn- job-done? [{:keys [label]}]
  (= ::done label))

(defn move-to
  "Updates the current state to the one with the given label"
  [state label]
  (assoc state :label label))

(defn state-label
  "Returns the label of the current job state"
  [state]
  (:label state))

(defn unknown-label [{:keys [label] :as state}]
  (throw (ex-info (str "Unknown label " label)
                  {:state state})))

(defprotocol StateMachine
  "Represents a state machine for a draftset update job."
  (create-initial-state [this live->draft source]
    "Creates the initial state to execute given a statement source and the current
     live->draft graph mapping for the current draftset")
  (step [this state context]
    "Execute a single transition of this machine from the given current state and
     return the transitioned-to state."))

(defn exec-state-machine-job
  "Executes the given state machine asynchronously within a job. Each state transition
   is queued as a continuation until the job is completed with the final result."
  [sm live->draft source context job]
  (letfn [(step-job [state job]
            (let [next-state (step sm state context)]
              (if (job-done? next-state)
                (if-let [result (::result next-state)]
                  (ajobs/job-succeeded! job result)
                  (ajobs/job-succeeded! job))
                (let [next-job (ajobs/create-child-job job (partial step-job next-state))]
                  (writes/queue-job! next-job)))))]
    (let [initial-state (create-initial-state sm live->draft source)]
      (step-job initial-state job))))

(defn exec-state-machine-sync
  "Executes the given state machine synchronously and returns the result"
  [sm live->draft source context]
  (let [initial-state (create-initial-state sm live->draft source)]
    (loop [next-state (step sm initial-state context)]
      (if (job-done? next-state)
        (::result next-state)
        (recur (step sm next-state context))))))

(defn create-state-machine-job
  "Creates a draftset update job for a user within a draftset for a source of quads and an
   update state machine."
  [{:keys [backend] :as manager} user-id draftset-ref source job-metadata sm]
  (jobs/make-job user-id
                 :background-write
                 job-metadata
                 (fn [job]
                   (let [live->draft (ops/get-draftset-graph-mapping backend draftset-ref)
                         context (job-context manager draftset-ref)]
                     (exec-state-machine-job sm live->draft source context job)))))
