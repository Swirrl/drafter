(ns drafter-client.triplestore
  (:require [clojure.tools.logging :as log]
            [drafter-client.client :as client]
            [drafter-client.client.auth :as auth]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.repo :refer [make-repo]]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.repository :as repo
             :refer
             [ToConnection with-transaction]]
            [grafter.db.triplestore.impl :refer [GetQueryCache]]
            [grafter.db.triplestore.query :refer [EvaluationMethod]]
            [integrant.core :as ig])
  (:import grafter_2.rdf.protocols.IStatement
           java.io.Closeable))

(defn- ->quad [statement]
  (pr/->Quad (pr/subject statement)
             (pr/predicate statement)
             (pr/object statement)
             (pr/context statement)))

(defmacro ^:private with-ensure-draftset-tx
  [[tx store :as bindings] & body]
  `(if-let [~tx (some-> ~store .-transaction deref)]
     (do ~@body)
     (with-transaction ~store
       (when-let [~tx (some-> ~store .-transaction deref)]
         (do ~@body)))))

(defn- ensure-can-begin [this]
  (when-let [tx (some-> this .-transaction deref)]
    (throw (ex-info "Cannot `begin` before previous transaction is `commit`ed"
                    {:transaction tx}))))

(defn- ensure-can-commit [this]
  (when-not (some-> this .-transaction deref)
    (throw (Exception. "Cannot `commit` transaction that has not begun"))))

(defn- ensure-complete
  ([message result]
   (ensure-complete message {} result))
  ([message map result]
   (when (client/job-failure-result? result)
     (throw (ex-info message {:data map :result result})))))

(defprotocol IGraphDrop
  (drop-graph [this graph]))

(deftype DrafterConnection [transaction client token context -conn]
  ;;; NOTE: This is not threadsafe as it expects the state of `transaction` to
  ;;; not have been mutated outside of the `begin` / `commit` / `rollback`
  ;;; phases. Use each instance only in a single threaded context! You cannot
  ;;; `begin` a transaction before a previous transaction is `commit`ed or a
  ;;; `rollback` has been performed.

  Closeable
  (close [_]
    (.close -conn)
    (reset! transaction nil))

  ToConnection
  (->connection [this] this)

  repo/IPrepareQuery
  (prepare-query* [this sparql-string restriction]
    (repo/prepare-query* -conn sparql-string restriction))

  pr/ISPARQLable
  (pr/query-dataset [this sparql-string model]
    (pr/query-dataset -conn sparql-string model))

  (pr/query-dataset [this sparql-string model opts]
    (pr/query-dataset -conn sparql-string model opts))

  pr/ITransactable
  (begin [this]
    (ensure-can-begin this)
    (->> (client/new-draftset client token "DrafterTriplestore TX draftset" "")
         (reset! transaction)))

  (commit [this]
    (ensure-can-commit this)
    (let [tx @transaction]
      (->> tx
           (client/publish client token)
           (client/wait-result! client token)
           (ensure-complete "Unable to `commit` transaction" {:transaction tx})))
    (reset! transaction nil))

  (rollback [_]
    (when-let [tx @transaction]
      (->> tx
           (client/remove-draftset client token)
           (client/wait-result! client token)
           (ensure-complete "Unable to `rollback` transaction" {:transaction tx}))
      (reset! transaction nil)))

  pr/ITripleWriteable
  (pr/add-statement [this statement]
    (with-ensure-draftset-tx [tx this]
      (->> [(->quad statement)]
           (client/add client token tx)
           (client/wait-nil! client token))))

  (pr/add-statement [this graph statement]
    (with-ensure-draftset-tx [tx this]
      (->> [(->quad statement)]
           (client/add client token tx graph)
           (client/wait-nil! client token))))

  (pr/add [this triples]
    (with-ensure-draftset-tx [tx this]
      (if (not (instance? IStatement triples))
        (when (seq triples)
          (let [quads (map ->quad triples)]
            (->> quads
                 (client/add client token tx)
                 (client/wait-nil! client token))))
        (pr/add-statement this triples)))
    this)

  (pr/add [this graph triples]
    (with-ensure-draftset-tx [tx this]
      (if (not (instance? IStatement triples))
        (when (seq triples)
          (let [quads (map ->quad triples)]
            (->> quads
                 (client/add client token tx graph)
                 (client/wait-nil! client token))))
        (pr/add-statement this graph triples)))
    this)

  ;; NOTE: Don't think these arities make sense in the context of drafter-client
  ;; The drafter API doesn't accept a raw stream of triples, so would need to be
  ;; converted into one of the above triple/quad encodings
  (pr/add [this graph format triple-stream]
    (throw (UnsupportedOperationException.
            "DrafterConnection does not support triple-streams")))

  (pr/add [this graph base-uri format triple-stream]
    (throw (UnsupportedOperationException.
            "DrafterConnection does not support triple-streams")))

  pr/ITripleDeleteable
  (delete-statement [this statement]
    (with-ensure-draftset-tx [tx this]
      (->> [(->quad statement)]
           (client/delete-quads client token tx)
           (client/wait-nil! client token))))

  (delete-statement [this graph statement]
    (with-ensure-draftset-tx [tx this]
      (->> [(->quad statement)]
           (client/delete-triples client token tx graph)
           (client/wait-nil! client token))))

  (delete [this quads]
    (with-ensure-draftset-tx [tx this]
      (if (not (instance? IStatement quads))
        (when (seq quads)
          (let [quads (map ->quad quads)]
            (->> quads
                 (client/delete-quads client token tx)
                 (client/wait-nil! client token))))
        (pr/delete-statement this quads))))

  (delete [this graph triples]
    (with-ensure-draftset-tx [tx this]
      (if (not (instance? IStatement triples))
        (when (seq triples)
          (let [quads (map ->quad triples)]
            (->> quads
                 (client/delete-triples client token tx graph)
                 (client/wait-nil! client token))))
        (pr/delete-statement this graph triples))))

  IGraphDrop
  (drop-graph [this graph]
    (with-ensure-draftset-tx [tx this]
      (client/delete-graph-2-sync client token tx graph))))

;; Don't use these, or the Class constructor, only use the constructor functions
(ns-unmap *ns* '->DrafterConnection)
(ns-unmap *ns* 'map->DrafterConnection)


(deftype DrafterRepository [client token context -repo]

  ToConnection
  (->connection [this]
    (DrafterConnection. (atom nil) client token context (.getConnection -repo)))

  pr/ITripleWriteable
  (pr/add-statement [this statement]
    (with-open [connection (repo/->connection this)]
      (log/debug "Opening connection" connection "on repo" this)
      (pr/add-statement connection statement)
      (log/debug "Closing connection" connection "on repo" this)
      this))

  (pr/add-statement [this graph statement]
    (with-open [connection (repo/->connection this)]
      (log/debug "Opening connection" connection "on repo" this)
      (pr/add-statement connection graph statement)
      (log/debug "Closing connection" connection "on repo" this)
      this))

  (pr/add [this triples]
    (with-open [connection (repo/->connection this)]
      (log/debug "Opening connection" connection "on repo" this)
      (pr/add connection triples)
      (log/debug "Closing connection" connection "on repo" this))
    this)

  (pr/add [this graph triples]
    (with-open [connection (repo/->connection this)]
      (log/debug "Opening connection" connection "on repo" this)
      (pr/add connection graph triples)
      (log/debug "Closing connection" connection "on repo" this)
      this))

  (pr/add [this graph format triple-stream]
    (with-open [connection (repo/->connection this)]
      (pr/add connection graph format triple-stream))
    this)

  (pr/add [this graph base-uri format triple-stream]
    (with-open [connection (repo/->connection this)]
      (pr/add connection graph base-uri format triple-stream))
    this))

;; Don't use these, or the Class constructor, only use the constructor functions
(ns-unmap *ns* '->DrafterRepository)
(ns-unmap *ns* 'map->DrafterRepository)


(deftype DrafterTriplestore [-conn repository]

  Closeable
  (close [_]
    (some-> @-conn .close)
    (reset! -conn nil))

  ToConnection
  (->connection [this]
    (or @-conn (reset! -conn (repo/->connection repository))))

  EvaluationMethod
  (evaluation-method [this]
    ;; only eager evaluation
    :eager)

  GetQueryCache
  (query-cache [_]
    ;; nope
    nil)

  pr/ITransactable
  (begin [this]
    (let [conn (repo/->connection this)]
      (pr/begin conn)))

  (commit [this]
    (pr/commit @-conn)
    (.close @-conn)
    (reset! -conn nil))

  (rollback [_]
    (when @-conn
      (pr/rollback @-conn)
      (.close @-conn))
    (reset! -conn nil))

  pr/ITripleWriteable
  (pr/add-statement [this statement]
    (pr/add-statement repository statement))

  (pr/add-statement [this graph statement]
    (pr/add-statement repository graph statement))

  (pr/add [this triples]
    (pr/add repository triples))

  (pr/add [this graph triples]
    (pr/add repository graph triples)))

;; Don't use these, or the Class constructor, only use the constructor functions
(ns-unmap *ns* '->DrafterTriplestore)
(ns-unmap *ns* 'map->DrafterTriplestore)

(defn repository
  ([client token context]
   (repository client token context nil))
  ([client token context http-client-session-manager]
   (let [repo (make-repo client context token {})]
     (when http-client-session-manager
       ;; Providing an HTTP client session manager allows us to use the same
       ;; underlying HTTP client for multiple repositories, so we can take
       ;; advantage of connection pooling. You probably want an
       ;; org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager
       (.setHttpClientSessionManager repo http-client-session-manager))
     (DrafterRepository. client token context repo))))

(defn triplestore [repository]
  (DrafterTriplestore. (atom nil) repository))

(defn auth-code-triplestore
  ([client token context]
   (auth-code-triplestore client token context nil))
  ([client token context http-client-session-manager]
   (triplestore (repository client token context http-client-session-manager))))

(defn m2m-triplestore
  ([client job-timeout context]
   (m2m-triplestore client job-timeout context))
  ([client job-timeout context http-client-session-manager]
   (let [token (:access_token (auth/get-client-id-token client))
         context (or context draftset/live)]
     (-> client
         (client/with-job-timeout job-timeout)
         (repository token context http-client-session-manager)
         (triplestore)))))

(defn mock-m2m-triplestore
  ([client mock-token job-timeout]
   (mock-m2m-triplestore client mock-token job-timeout nil))
  ([client mock-token job-timeout http-client-session-manager]
   (let [context draftset/live]
     (-> client
         (client/with-job-timeout job-timeout)
         (repository mock-token context http-client-session-manager)
         (triplestore)))))

(defmethod ig/init-key :drafter-client.triplestore/auth-code-triplestore
  [_ {:keys [client http-client-session-manager]}]
  (fn [token draft]
    (auth-code-triplestore client token draft http-client-session-manager)))

(defmethod ig/init-key :drafter-client.triplestore/m2m-triplestore
  [_ {:keys [client job-timeout http-client-session-manager]
      :or {job-timeout ##Inf}}]
  (m2m-triplestore client job-timeout nil http-client-session-manager))

(defmethod ig/init-key :drafter-client.triplestore/m2m-draft-triplestore
  [_ {:keys [client job-timeout http-client-session-manager]
      :or {job-timeout ##Inf}}]
  (fn [draft]
    (m2m-triplestore client job-timeout draft http-client-session-manager)))

(defmethod ig/init-key :drafter-client.triplestore/mock-m2m-triplestore
  [_ {:keys [client token job-timeout http-client-session-manager]
      :or {job-timeout ##Inf}}]
  (mock-m2m-triplestore client token job-timeout http-client-session-manager))

(defmethod ig/halt-key! :drafter-client.triplestore/auth-code-triplestore
  [_ triplestore]
  ;; this is a function so can't really have any open connections
  )

(defmethod ig/halt-key! :drafter-client.triplestore/m2m-triplestore
  [_ triplestore]
  (some-> triplestore .close))

(defmethod ig/halt-key! :drafter-client.triplestore/mock-m2m-triplestore
  [_ triplestore]
  (some-> triplestore .close))
