(ns drafter.feature.draftset.update
  "Handler for submitting (a limited subset of) SPARQL UPDATE queries
  within a draft.

  When interpreting a request the incoming UPDATE request is parsed as
  a series of 'high-level' update operations with Jena, and rejected
  if it contains any operations outside of the supported subset
  `INSERT DATA`, `DELETE DATA` and `DROP GRAPH`.

  Prior to planning an update, we first assemble some supporting
  metadata for all of the graphs affected by the query. This is done
  in `get-graph-meta`, this information includes the (live) :graph-uri
  and :draft-graph-uri, along with information on the `:live-size` and
  `:draft-size` and the `:state` of the graph within the draftset. The
  graph size information is used to determine whether the update is
  small enough to occur and not adversely affect the performance of
  other operations. The `:state` is a reflection of that graphs state
  in the system, i.e. whether it is `:unmanaged`, `:live` or `:draft`.
  From the perspective of the draft, graphs are either `:unmanaged`,
  only in the `:live` graph or in the `:draft` graph. Graphs that have
  existing draft graphs within the draftset do not need special
  handling, but unmanaged or live-only graphs require prior handling
  before the operation can be applied such as creating the draft
  graph, cloning and rewriting live graphs etc.

  Using this metadata and state information, each high-level Jena
  operation is then mapped to a planned 'intermediate-level' sequence
  of 'abstract operations' which represent what effects need to happen
  to correctly transition the drafter state graph and draft states
  accordingly. Intermediate operations are things like
  `:create-new-draft`, `:clone`, `:rewrite` etc...

  These intermediate operations are reified with `reify-operation`
  into low-level update fragments, that are concatenated into the
  final SPARQL update string via the JENA APIs, with `build-update`.
  Reified operations can be either a string or a JENA update object.
  "
  (:require [clojure.set :as set]
            [drafter.backend.draftset.arq :as arq]
            [drafter.backend.draftset.draft-management :as dm]
            [drafter.backend.draftset.rewrite-query :refer [uri-constant-rewriter]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.draftset :as ds]
            [drafter.rdf.sparql :as sparql]
            [integrant.core :as ig]
            [ring.middleware.cors :as cors]
            [drafter.errors :refer [wrap-encode-errors]]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.url :as url]
            [clojure.string :as str]
            [ring.util.request :as request]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.time :as time]
            [drafter.rdf.jena :as jena]
            [drafter.feature.modified-times :as modified-times]
            [clojure.tools.logging :as log])
  (:import java.net.URI
           [org.apache.jena.sparql.modify.request
            QuadDataAcc Target
            UpdateCopy UpdateDrop UpdateDataDelete UpdateDataInsert]
           org.apache.jena.sparql.sse.SSE
           org.apache.jena.sparql.sse.Item
           [org.apache.jena.query Syntax]
           [org.apache.jena.update UpdateFactory UpdateRequest]
           [org.apache.jena.sparql.core Quad]
           [org.apache.jena.graph Node]))

(defn- rewrite-quad [rewriter ^Quad quad]
  (-> quad SSE/str arq/->sse-item arq/sse-zipper rewriter str SSE/parseQuad))

(defprotocol UpdateOperation
  (affected-graphs [op]
    "Returns the set of (live) graph URIs affected by this operation")
  (size [op]
    "Return the number graphs affected by this operation")
  (abstract-operations [op graph-meta max-update-size]
    "Returns the sequence of abstract operations required to apply this update
     operation given the current graph states. Can throw if this operation is
     too large to be carried out.")
  (rewrite [op rewriter]
    "Returns this operation rewritten with the given rewriter"))

(defn- forbidden-request [& [msg]]
  (ex-info (str "403 Forbidden" (when msg (str ": " msg))) {:error :forbidden}))

(defn- unprocessable-request [unprocessable]
  (ex-info "422 Unprocessable Entity" {:error :unprocessable-request}))

(defn- payload-too-large [operations]
  (ex-info "413 Payload Too Large." {:error :payload-too-large}))

(defn- rewrite-draftset-ops [draftset-id]
  (-> {:draftset-uri (ds/->draftset-uri draftset-id)}
      dm/rewrite-draftset-q
      UpdateFactory/create
      .getOperations))

(defn- draft-graph-stmt [graph-manager draftset-ref timestamp graph-uri draft-graph-uri]
  (-> (graphs/new-draft-user-graph-statements graph-manager graph-uri draft-graph-uri timestamp draftset-ref)
      (jena/insert-data-stmt)))

(defn- manage-graph-stmt [graph-manager graph-uri]
  (-> (graphs/new-managed-user-graph-statements graph-manager graph-uri)
      (jena/insert-data-stmt)))

(defn- copy-graph-stmt [graph-uri draft-graph-uri]
  (UpdateCopy. (Target/create (str graph-uri))
               (Target/create (str draft-graph-uri))))

(defn- within-limit? [operations max-update-size]
  (<= (reduce + (map size operations)) max-update-size))

(defn- processable? [operation]
  (satisfies? UpdateOperation operation))

(defn- get-live-graphs [backend graphs]
  (->> (format "SELECT ?lg
                FROM <http://publishmydata.com/graphs/drafter/drafts>
                WHERE {
                  VALUES ?lg { %s }
                  ?lg a <http://publishmydata.com/def/drafter/ManagedGraph> .
                  ?lg <http://publishmydata.com/def/drafter/isPublic> true .
                }"
               (str/join " " (map #(str "<" % ">") graphs)))
       (sparql/eager-query backend)
       (map (fn [{lg :lg}]
              [lg {:graph-uri lg
                   :draft-graph-uri (dm/make-draft-graph-uri)
                   :state :live
                   :draft? false
                   :live? true}]))
       (into {})))

(defn- get-draft-graphs [backend draftset-id]
  (->> (format "SELECT ?lg ?dg
                FROM <http://publishmydata.com/graphs/drafter/drafts>
                WHERE {
                  ?dg <http://publishmydata.com/def/drafter/inDraftSet> <%s> .
                  ?lg a <http://publishmydata.com/def/drafter/ManagedGraph> ;
                      <http://publishmydata.com/def/drafter/hasDraft> ?dg .
                }"
               (ds/->draftset-uri draftset-id))
       (sparql/eager-query backend)
       (map (fn [{:keys [lg dg]}]
              [lg {:graph-uri lg
                   :draft-graph-uri dg
                   :state :draft
                   :draft? true
                   :live? true}]))
       (into {})))

(defn- graph-size [backend graph-uri max-update-size]
  (:c (first (sparql/eager-query backend (format "SELECT (COUNT(*) AS ?c)
                                                  FROM <%s>
                                                  WHERE {
                                                    SELECT *
                                                    WHERE { ?s ?p ?o }
                                                    LIMIT %d
                                                  }"
                                                 graph-uri
                                                 (inc max-update-size))))))

(defn- get-graph-meta
  "Returns a map of live graph URI => graph state for all the graphs affected by an
   UPDATE request within a draftset. Draft graph URIs are generated for new graphs
   and these must be created by the relevant state graph management operations."
  [backend draftset-id ^UpdateRequest update-request max-update-size]
  (let [new-graph (fn [lg]
                    [lg {:graph-uri lg
                         :draft-graph-uri (dm/make-draft-graph-uri)
                         :state :unmanaged
                         :draft? false
                         :live? false}])
        affected-graphs (->> (.getOperations update-request)
                             (map (juxt identity affected-graphs)))
        affected-graphs (->> (map second affected-graphs)
                             (apply set/union)
                             (map new-graph)
                             (into {}))]
    (->> (merge-with merge
                     affected-graphs
                     (get-live-graphs backend (keys affected-graphs))
                     (get-draft-graphs backend draftset-id))
         (map (fn [[lg {dg :draft-graph-uri :as meta}]]
                [lg (assoc meta
                           :draft-size (graph-size backend dg max-update-size)
                           :live-size (graph-size backend lg max-update-size))]))
         (into {}))))

;; abstract operations

(defmulti reify-operation
          "Converts an abstract operation (make draft, clone graph etc.) into a concrete sequence of JenaUpdateOperations
           that comprise an UPDATE query."
          (fn [abstract-op _update-context] (:type abstract-op)))

(defn- create-new-draft-op
  "Represents creating a draft for a new graph i.e. one which does not already exist in live."
  [graph-uri draft-graph-uri]
  {:type :create-new-draft :graph-uri graph-uri :draft-graph-uri draft-graph-uri})

(defmethod reify-operation :create-new-draft [{:keys [graph-uri draft-graph-uri] :as abstract-op} {:keys [graph-manager timestamp draftset-ref] :as update-context}]
  [(manage-graph-stmt graph-manager graph-uri)
   (draft-graph-stmt graph-manager draftset-ref timestamp graph-uri draft-graph-uri)])

(defn- create-live-draft-op
  "Represents creating a draft for an existing live graph"
  [graph-uri draft-graph-uri]
  {:type :create-live-draft :graph-uri graph-uri :draft-graph-uri draft-graph-uri})

(defmethod reify-operation :create-live-draft [{:keys [graph-uri draft-graph-uri] :as abstract-op} {:keys [graph-manager timestamp draftset-ref]}]
  [(draft-graph-stmt graph-manager draftset-ref timestamp graph-uri draft-graph-uri)])

(defn- clone-graph-op
  "Represents cloning a live graph into the given draft graph"
  [graph-uri draft-graph-uri]
  {:type :clone :graph-uri graph-uri :draft-graph-uri draft-graph-uri})

(defmethod reify-operation :clone [{:keys [graph-uri draft-graph-uri]} _update-context]
  [(copy-graph-stmt graph-uri draft-graph-uri)])

(defn- rewrite-draftset-op
  "Represents an operation to rewrite live resources to their draft representation. This should be carried out
   after a consecutive sequence of live graph clones"
  []
  {:type :rewrite-draftset})

(defmethod reify-operation :rewrite-draftset [_op {:keys [draftset-ref] :as update-context}]
  (rewrite-draftset-ops draftset-ref))

(defn- rewrite-op
  "Represents rewriting a Jena UpdateOperation"
  [op]
  {:type :rewrite :op op})

(defmethod reify-operation :rewrite [{:keys [op]} {:keys [rewriter] :as update-context}]
  [(rewrite op rewriter)])

(defn touch-graph-op
  "Represents updating the modification time for a draft graph"
  [draft-graph-uri]
  {:type :touch :draft-graph-uri draft-graph-uri})

(defn- insert-delete-graph-operations
  "Returns the sequence of abstract operations required to setup a DELETE/INSERT DATA operation
   before it can be applied. Live graphs which do not exist within the draft must be cloned
   and should be rejected if they are too large. Unmanaged graphs must be created with new
   draft graphs."
  [{:keys [state graph-uri draft-graph-uri live-size]} max-update-size]
  (case state
    :draft [(touch-graph-op draft-graph-uri)]
    :live (if (> live-size max-update-size)
            (throw (ex-info "Unable to copy graphs" {:type :error}))
            [(create-live-draft-op graph-uri draft-graph-uri)
             (clone-graph-op graph-uri draft-graph-uri)])
    :unmanaged [(create-new-draft-op graph-uri draft-graph-uri)]
    (throw (ex-info "Unknown state" {:graph-state state}))))

(defn- data-insert-delete-ops [op affected-graphs graph-states max-update-size]
  (let [affected-graph-states (select-keys graph-states affected-graphs)
        affected-draft-graphs (set (map :draft-graph-uri (vals affected-graph-states)))
        setup-ops (mapcat (fn [gm] (insert-delete-graph-operations gm max-update-size)) (vals affected-graph-states))

        ;; need to rewrite live data after any clone operations
        is-clone-op? (fn [op] (= :clone (:type op)))
        does-clone? (boolean (some is-clone-op? setup-ops))
        rewrite-ds (when does-clone? [(rewrite-draftset-op)])
        touch-ops (map touch-graph-op affected-draft-graphs)]
    (concat setup-ops rewrite-ds [(rewrite-op op)] touch-ops)))

;; update plan

(defn- transition-graph-state
  "Transitions a graph state from an expected current state to a new state. Throws an exception if the
   current graph state differs from the expected state."
  [plan graph-uri from to]
  (letfn [(transition [current-state]
            (if (= current-state from)
              to
              (let [msg (format "Unexpected graph state - expected %s but found %s" (name from) (name current-state))
                    info {:from from :to to :current current-state}]
                (throw (ex-info msg info)))))]
    (update-in plan [:graph-meta graph-uri :state] transition)))

(defn- add-to-plan
  "Adds an abstract operation to the current update plan"
  [plan {:keys [type] :as abstract-op}]
  (letfn [(add-operation [plan abstract-op]
            (update plan :operations conj abstract-op))
          (add-touch [plan {:keys [draft-graph-uri] :as touch-op}]
            (update plan :draft-graphs-to-touch conj draft-graph-uri))]
    (case type
      :create-new-draft (-> plan
                            (add-operation abstract-op)
                            (transition-graph-state (:graph-uri abstract-op) :unmanaged :draft))
      :create-live-draft (-> plan
                             (add-operation abstract-op)
                             (transition-graph-state (:graph-uri abstract-op) :live :draft))
      :touch (add-touch plan abstract-op)
      (add-operation plan abstract-op))))

(defn create-empty-plan [graph-meta]
  {:operations []
   :draft-graphs-to-touch #{}
   :graph-meta graph-meta})

(defn- empty-plan? [{:keys [operations draft-graphs-to-touch]}]
  (and (empty? operations) (empty? draft-graphs-to-touch)))

(defn- plan-update
  "Constructs an update plan from a sequence of Jena Update operations and the state of all affected graphs.
   Each Update operation is processed in order and the corresponding abstract operations are used to update the
   current plan. The subsequent state of graphs within the draft (unmanaged, draft, live) may be changed as a
   result."
  [operations graph-meta max-update-size]
  (let [empty-plan (create-empty-plan graph-meta)]
    (reduce (fn [{:keys [graph-meta] :as plan} op]
              (let [abstract-ops (abstract-operations op graph-meta max-update-size)]
                (reduce add-to-plan plan abstract-ops)))
            empty-plan
            operations)))

(defn- quad-graph-uri [^Quad q]
  (URI. (.getURI (.getGraph q))))

(extend-protocol UpdateOperation
  UpdateDataDelete
  (affected-graphs [^UpdateDataDelete op]
    (set (map quad-graph-uri (.getQuads op))))
  (size [op]
    (count (.getQuads op)))
  (abstract-operations [op graph-states max-update-size]
    (data-insert-delete-ops op (affected-graphs op) graph-states max-update-size))
  (rewrite [^UpdateDataDelete op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataDelete.)))

  UpdateDataInsert
  (affected-graphs [^UpdateDataInsert op]
    (set (map quad-graph-uri (.getQuads op))))
  (size [^UpdateDataInsert op]
    (count (.getQuads op)))
  (abstract-operations [op graph-states max-update-size]
    (data-insert-delete-ops op (affected-graphs op) graph-states max-update-size))
  (rewrite [op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataInsert.)))

  UpdateDrop
  (affected-graphs [op]
    #{(URI. (.getURI (.getGraph op)))})
  (size [_op] 1)
  (abstract-operations [^UpdateDrop op graph-meta max-update-size]
    (let [graph-uri (URI. (.getURI (.getGraph op)))
          {:keys [draft-graph-uri state draft-size] :as graph-info} (get graph-meta graph-uri)]
      (cond (= :draft state) (if (<= draft-size max-update-size)
                               [(rewrite-op op)
                                (touch-graph-op draft-graph-uri)]
                               (throw (ex-info "Unable to copy graphs" {:type :error})))
            (= :live state) [(create-live-draft-op graph-uri draft-graph-uri)
                             (touch-graph-op draft-graph-uri)]
            (.isSilent op) []
            :not-silent (throw (ex-info (format "Source graph %s does not exist, cannot proceed with update." graph-uri)
                                        {:type :error})))))

  (rewrite [op rewriter]
    (let [^Node node (-> op .getGraph Item/createNode arq/sse-zipper rewriter)]
      (UpdateDrop. node (.isSilent op)))))

(defn- rewriter-map [graph-meta]
  (->> graph-meta
       (map (juxt (comp str key) (comp str :draft-graph-uri val)))
       (into {})))

(defn build-update
  "Converts an update plan into a SPARQL UPDATE string"
  [{:keys [operations draft-graphs-to-touch] :as update-plan} {:keys [backend graph-manager draftset-ref timestamp] :as update-context}]
  (let [data-ops (mapcat (fn [op] (reify-operation op update-context)) operations)
        update-operations (modified-times/update-modifications-queries backend graph-manager draftset-ref draft-graphs-to-touch timestamp)]
    (jena/->update-string (concat data-ops update-operations))))

(defn update! [{:keys [backend graph-manager clock] :as manager} max-update-size draftset-id ^UpdateRequest update-request]
  (let [graph-meta (get-graph-meta backend
                                   draftset-id
                                   update-request
                                   max-update-size)
        update-plan (plan-update (.getOperations update-request) graph-meta max-update-size)]
    (when-not (empty-plan? update-plan)
      (let [rewriter-map (rewriter-map graph-meta)
            rewriter (partial uri-constant-rewriter rewriter-map)

            ;; represents the static properties of the environment the update is
            ;; executed within
            update-context {:rewriter        rewriter
                            :draftset-ref    (ds/->DraftsetId draftset-id)
                            :timestamp       (time/now clock)
                            :backend         backend
                            :graph-manager   graph-manager
                            :max-update-size max-update-size}

            update-request' (build-update update-plan update-context)]
        (log/debugf "About to run rewritten draft UPDATE query: %n%s" update-request')
        (sparql/update! backend update-request')))))

;; Handler

(defn- parse-update-params [{:keys [body query-params form-params] :as request}]
  (case (request/content-type request)
    "application/x-www-form-urlencoded"
    {:update (get form-params "update")
     :using-graph-uri (get form-params "using-graph-uri")
     :using-named-graph-uri (get form-params "using-named-graph-uri")
     :from "'update' form parameter"}
    "application/sparql-update"
    {:update body
     :using-graph-uri (get query-params "using-graph-uri")
     :using-named-graph-uri (get query-params "using-named-graph-uri")
     :from "body"}
    (throw (ex-info "Bad request" {:error :bad-request}))))

(defn- parse-draftset-id [request]
  (or (some-> request :route-params :id)
      (throw (ex-info "Parameter draftset-id must be provided"
                      {:error :bad-request}))))

(defn- protected? [{:keys [graph-manager] :as manager} op]
  (some (partial graphs/protected-graph? graph-manager) (affected-graphs op)))

(defn- ^UpdateRequest parse-update-string [^String update-string]
  (UpdateFactory/create update-string Syntax/syntaxSPARQL_11))

(defn parse-update-query
  "Parses and validates an UPDATE query string. Return the parsed query representation."
  [manager update-query-string max-update-size]
  (let [update-request (parse-update-string update-query-string)
        operations (.getOperations update-request)
        unprocessable (remove processable? operations)]
    (cond (some (partial protected? manager) operations)
          (throw (forbidden-request "Protected graphs in update request"))
          (seq unprocessable)
          (throw (unprocessable-request unprocessable))
          (not (within-limit? operations max-update-size))
          (throw (payload-too-large operations))
          :else
          update-request)))

(defn- get-update-query
  "Extracts an UPDATE query string from a representation of the input request parameters"
  [{:keys [update from]}]
  (cond (string? update)
        update
        (instance? java.io.InputStream update)
        (slurp update)
        (coll? update)
        (throw (ex-info "Exactly one query parameter required"
                        {:error :unprocessable-request}))
        :else
        (throw (ex-info (str "Expected SPARQL query in " from)
                        {:error :unprocessable-request}))))

#_(defn- prepare-update [backend request]
  ;; NOTE: Currently unused. None of the Update types that we have implemented
  ;; support `using-(named-)graph-uri`.
  (with-open [repo (repo/->connection backend)]
    (let [params (parse-update-params request)
          default-graph (:using-graph-uri params)
          named-graphs (:using-named-graph-uri params)
          dataset (repo/make-restricted-dataset :default-graph default-graph
                                                :named-graphs named-graphs)]
      (doto (.prepareUpdate repo (:update params))
        (.setDataset dataset)))))

(defn- parse-update [manager request max-update-size]
  (let [{:keys [request-method]} request]
    (if (= request-method :post)
      (let [draftset-id (parse-draftset-id request)
            params (parse-update-params request)
            update-query (get-update-query params)
            update-request (parse-update-query manager update-query max-update-size)]
        (log/infof "Received draft UPDATE query: %n%s" update-query)
        (assoc params :update-request update-request :draftset-id draftset-id))
      (throw (ex-info "Method not supported"
                      {:error :method-not-allowed :method request-method})))))

(defn- handler*
  [{:keys [drafter/manager max-update-size] :as opts} request]
  ;; TODO: write-lock?
  (let [{:keys [update-request draftset-id]} (parse-update manager request max-update-size)]
    (update! manager max-update-size draftset-id update-request)
    {:status 204}))

(defn handler
  [{:keys [wrap-as-draftset-owner] :as opts}]
  (wrap-as-draftset-owner :drafter:draft:edit (fn [request] (handler* opts request))))

(def cors-allowed-headers
  #{"Accept"
    "Accept-Encoding"
    "Authorization"
    "Cache-Control"
    "Content-Type"
    "DNT"
    "If-Modified-Since"
    "Keep-Alive"
    "User-Agent"
    "X-CustomHeader"
    "X-Requested-With"})

(defmethod ig/init-key ::handler [_ opts]
  (-> (handler opts)
      (wrap-encode-errors)
      (cors/wrap-cors :access-control-allow-headers cors-allowed-headers
                      :access-control-allow-methods [:get :options :post]
                      :access-control-allow-origin #".*")))
