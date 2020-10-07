(ns drafter-client.client
  (:refer-clojure :exclude [name type get])
  (:require [cheshire.core :as json]
            [clj-time.format :refer [formatters parse]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [swirrl.auth0.client :as auth]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.interceptors :as interceptor]
            [drafter-client.client.impl :as i]
            [drafter-client.client.protocols :refer [->DrafterClient]]
            [drafter-client.client.repo :as repo]
            [drafter-client.client.endpoint :as endpoint]
            [drafter-client.client.util :refer [uuid date-time]]
            [integrant.core :as ig]
            [martian.clj-http :as martian-http]
            [martian.core :as martian])
  (:import clojure.lang.ExceptionInfo))

(alias 'c 'clojure.core)

(def live draftset/live)

(defn exception? [v]
  (instance? Exception v))

(defn ->job [m]
  (-> m
      (update :id uuid)
      (update :status keyword)
      (update :priority keyword)
      (update :start-time date-time)
      (update :finish-time date-time)
      (update :draftset-id (comp uuid :id))
      (update :draft-graph-id uuid)))

(defrecord AsyncJob [job-id restart-id])

(defn- ->async-job [{:keys [finished-job restart-id] :as rsp}]
  (let [job-id (-> finished-job (str/split #"/") last uuid)]
    (->AsyncJob job-id (java.util.UUID/fromString restart-id))))

(defn job-failed? [job-state]
  (contains? job-state :error))

(defn job-succeeded? [{:keys [status] :as job-state}]
  (and (not (job-failed? job-state))
       (= :complete status)))

(defn job-complete? [job-state]
  (or (job-failed? job-state)
      (job-succeeded? job-state)))

(defn job-in-progress? [{:keys [status] :as job-state}]
  (= :pending status))

(defn drafter-restarted?
  "Whether drafter restarted between two polled states of a job."
  [job state]
  {:pre [(some? (:restart-id job))
         (some? (:restart-id state))]}
  (not= (:restart-id job) (:restart-id state)))

(defn- job-details [job-state]
  (:details job-state))

(defn ->repo [client access-token context]
  (repo/make-repo client context access-token {}))

(defn endpoints
  "Get all endpoints visible to the user represented by the optional access-token.
   If access-token is nil, only the publicly-visible endpoints will be returned. The
   list of endpoints can be constrained by the include parameter which behaves the
   same as for draftsets."
  [client access-token & [include]]
  (let [get-endpoints (partial i/request client i/get-endpoints access-token)
        include (if (keyword? include) (c/name include) include)
        endpoints (if include (get-endpoints :include include) (get-endpoints))]
    (map endpoint/from-json endpoints)))

(defn get-public-endpoint
  "Gets the public endpoint"
  [client]
  (endpoint/from-json (i/get-public-endpoint client)))

(defn draftsets
  "List available Draftsets. The optional opts map allows additional options to be provided
   to the request. The supported parameters are:
   - include: One of (:all :owned :claimable) specifying which draftsets to return
   - union-with-live: Whether to combine each draftset with the public endpoint"
  ([client access-token] (draftsets client access-token {}))
  ([client access-token opts]
   (->> (i/request client i/get-draftsets access-token opts)
        (map draftset/from-json))))

(defn new-draftset
  "Create a new Draftset"
  [client access-token name description]
  (-> client
      (i/request i/create-draftset access-token :display-name name :description description)
      (draftset/from-json)))

(defn get-draftsets
  "List available draftsets, optionally `:include` either
  `#{:all :owned :claimable}`.
  Returns all properties."
  [client access-token & [include]]
  (let [opts (if include {:include include} {})]
    (draftsets client access-token opts)))

(defn get-draftset
  ([client access-token id] (get-draftset client access-token id {}))
  ([client access-token id opts]
   (draftset/from-json (i/request client i/get-draftset access-token id opts))))

(defn get-endpoint
  "Fetches the specified endpoint from a reference to it. If the reference
   is to a draftset access-token must be specified."
  ([client access-token endpoint-ref] (get-endpoint client access-token endpoint-ref {}))
  ([client access-token endpoint-ref opts]
   (cond
     (draftset/draft? endpoint-ref)
     (get-draftset client access-token (draftset/id endpoint-ref) opts)

     (endpoint/public-ref? endpoint-ref)
     (get-public-endpoint client)

     :else
     (throw (ex-info "No get operation for endpoint" {:type :invalid-operation
                                                      :endpoint endpoint-ref})))))

(defn edit-draftset [client access-token id name description]
  (i/request client i/put-draftset access-token id
         :display-name name
         :description description))

(defn submit-to-user [client access-token id user]
  (i/request client i/submit-draftset-to access-token id :user user))

(defn submit-to-role [client access-token id role]
  (let [role (if (keyword? role) (c/name role) role)]
    (i/request client i/submit-draftset-to access-token id :role role)))

(defn claim [client access-token id]
  (i/request client i/claim-draftset access-token id))

(defn remove-draftset
  "Delete the Draftset and its data"
  ([client access-token draftset]
   (remove-draftset client access-token draftset {}))
  ([client access-token draftset {:keys [metadata]}]
   (-> client
       (i/request i/delete-draftset access-token (draftset/id draftset) {:metadata metadata})
       (->async-job))))

(defn load-graph
  "Load the graph from live into the Draftset"
  ([client access-token draftset graph]
   (load-graph client access-token draftset graph {}))
  ([client access-token draftset graph {:keys [metadata]}]
   (-> client
       (i/request i/put-draftset-graph
                  access-token
                  (draftset/id draftset)
                  (str graph)
                  {:metadata metadata})
       (->async-job))))

(defn delete-draftset-changes
  "Remove all changes from the named graph"
  [client access-token draftset graph]
  (i/request client i/delete-draftset-changes access-token
             (draftset/id draftset) (str graph)))

(defn delete-graph
  "Schedules the deletion of the graph from live.

  Takes an optional last argument of opts. Currently the supported
  opts are:

  :silent    - When true equivalent to SPARQL's DROP SILENT.  A
               boolean indicating whether or not to raise an error
               if the graph to be deleted doesn't exist.  If set to
               true the function will succeed without error if the
               graph being deleted doesn't exist.  Defaults to false.
  "
  ([client access-token draftset graph]
   (delete-graph client access-token draftset graph {}))
  ([client access-token draftset graph opts]
   (apply i/request
          client
          i/delete-draftset-graph
          access-token
          (draftset/id draftset)
          (str graph)
          (apply concat opts))))

(defn delete-quads
  ([client access-token draftset quads]
   (delete-quads client access-token draftset quads {}))
  ([client access-token draftset quads {:keys [metadata]}]
   (-> client
       (interceptor/set-content-type "application/n-quads")
       (i/request i/delete-draftset-data access-token (draftset/id draftset) quads {:metadata metadata})
       (->async-job))))

(defn delete-triples
  ([client access-token draftset graph triples]
   (delete-triples client access-token draftset graph triples {}))
  ([client access-token draftset graph triples opts]
   (let [{:keys [metadata]} opts]
     (-> client
         (interceptor/set-content-type "application/n-triples")
         (i/request i/delete-draftset-data
           access-token
           (draftset/id draftset)
           triples
           {:graph graph
            :metadata metadata})
         (->async-job)))))

(defn add-data
  "Append the supplied RDF statements to this Draftset.
  - `statements` can be a sequence of quads or triples, a File, or an InputStream
  - `opts` is a map of optional arguments, which may include:
    - `graph`: required if the statements are triples
    - `metadata`: a map with arbitrary keys that will be included on the job for future reference"
  ([client access-token draftset statements]
   (add-data client access-token draftset statements {}))
  ([client access-token draftset statements opts]
   (let [url (martian/url-for client
                              :put-draftset-data
                              {:id (draftset/id draftset)})
         format (i/get-format statements (opts :graph))]
     (-> (i/append-via-http-stream access-token
                                   url
                                   statements
                                   (assoc opts
                                          :format format
                                          :auth-provider (:auth-provider client)))
         (->async-job)))))

(defn add
  "Append the supplied RDF statements to this Draftset.
  'statements' can be a sequence of quads or triples; a File; or InputStream"
  {:deprecated "Use add-data instead"}
  ([client access-token draftset quads]
   (add-data client access-token draftset quads))
  ([client access-token draftset graph triples]
   (add-data client access-token draftset triples {:graph graph})))

(defn add-in-batches
  "Append the supplied RDF data to this Draftset in batches"
  {:deprecated "Use add-data instead. It can handle streaming arbitrarily large files directly to drafter."}
  ([{:keys [batch-size] :as client} access-token draftset quads]
   (->> quads
        (partition-all batch-size)
        (map (fn [quad-batch] (add client access-token draftset quad-batch)))
        doall))
  ([{:keys [batch-size] :as client} access-token draftset graph triples]
   (->> triples
        (partition-all batch-size)
        (map (fn [triple-batch] (add client access-token draftset graph triple-batch)))
        doall)))

(defn publish
  "Publish the Draftset to live"
  ([client access-token draftset]
   (publish client access-token draftset {}))
  ([client access-token draftset {:keys [metadata]}]
   (-> client
       (i/request i/publish-draftset access-token (draftset/id draftset) {:metadata metadata})
       (->async-job))))

(defn get
  "Access the quads inside this Draftset"
  ([client access-token draftset]
   (-> client
       (interceptor/accept "application/n-quads")
       (i/request i/get-draftset-data access-token (draftset/id draftset))))
  ([client access-token draftset graph]
   (-> client
       (interceptor/accept "application/n-triples")
       (i/request i/get-draftset-data access-token (draftset/id draftset) :graph graph))))

(defn job [client access-token id]
  (->job (i/request client i/get-job access-token id)))

(defn jobs [client access-token]
  (map ->job (i/request client i/get-jobs access-token)))

(defn- refresh-job
  "Poll to get the latest state of a job.

  Either returns the corresponding async-job object, or nil indicating
  the job wasn't found.

  HTTP protocol errors will cause this function to raise an exception."
  [client access-token {:keys [job-id] :as async-job}]
  (try
    (job client access-token job-id)
    (catch ExceptionInfo e
      (let [{:keys [status]} (ex-data e)]
        (if (= status 404)
          nil ;; job doesn't exist
          (throw e))))))

(def job-failure-result? exception?)

(defn job-status
  "Returns a value representing the result of the given async job or ::pending if the
   job is still in progress."
  [job job-state]
  (let [info {:job job :state job-state}]
    (cond
      (job-succeeded? job-state) job-state
      (job-failed? job-state) (ex-info "Job failed" info)
      (job-in-progress? job-state) ::pending
      :else (ex-info "Unknown job state" info))))

(defn job-timeout-exception [job]
  (ex-info "Timed out waiting for job to finish" {:type ::job-timeout :job job}))

(defn job-timeout-exception? [e]
  (-> e ex-data :type (= ::job-timeout)))

(defn- wait-opts [client]
  {:job-timeout (or (:job-timeout client) ##Inf)})

(defn wait-result!
  "Waits for an async job to complete and returns the result map if it
  succeeded or, returns an exception representing the failure
  otherwise.

  If a :job-timeout option is provided in opts (or set via
  a :job-timeout key on the client) then this call will raise
  a ::job-timeout exception if the job does not finish before the
  timeout.

  NOTE: That if a job does timeout, the timeout only occurs client
  side and the job itself will be left running against drafter."
  ([client access-token job]
   (wait-result! client access-token job (wait-opts client)))
  ([client access-token job {:keys [job-timeout]}]
   (loop [waited 0]
     (if-let [state (refresh-job client access-token job)]
       (let [status (job-status job state)
             wait 500]
         (cond (>= waited job-timeout)
               (job-timeout-exception job)
               (= ::pending status)
               (do (Thread/sleep wait) (recur (+ waited wait)))
               :else
               status))
       (ex-info "Job not found" job)))))

(defn wait-results!
  "Waits for a sequence of jobs to complete and returns a sequence of results in corresponding order.
   If a job succeeded, the result will be a result map, otherwise an exception indiciating the reason
   for the failure.

  Takes the same opts as wait-result!"
  ([client access-token jobs] (wait-results! client access-token jobs (wait-opts client)))
  ([client access-token jobs opts]
   (mapv #(wait-result! client access-token % opts) jobs)))

(defn wait!
  "Waits for the specified job to complete. Returns the result of the job if successful or
  throws an exception if the job failed.

  Takes the same opts as wait-result!"
  ([client access-token job] (wait! client access-token job (wait-opts client)))
  ([client access-token job opts]
   (let [result (wait-result! client access-token job opts)]
     (if (exception? result)
       (throw result)
       result))))

(defn wait-nil!
  "Waits for a job to complete and returns nil if successful, otherwise throws an exception

  Takes the same opts as wait-result!"
  ([client access-token job]
   (wait-nil! client access-token job (wait-opts client)))
  ([client access-token job opts]
   (wait! client access-token job opts)
   nil))

(defn wait-all!
  "Waits for the specified jobs to complete. Returns a sequence of the complete job results in the
   corresponding order of the jobs in the source sequence if they all completed successfully, or
   throws an exception if any of the jobs failed.

  Takes the same opts as wait-result!"
  ([client access-token jobs]
   (wait-all! client access-token jobs (wait-opts client)))
  ([client access-token jobs opts]
   (let [results (wait-results! client access-token jobs opts)
         failures (filter exception? results)
         succeeded (remove exception? results)]
     (if (seq failures)
       (throw (ex-info "One or more jobs failed" {:failed    failures
                                                  :succeeded succeeded}))
       succeeded))))

(defn resolve-job
  "Wait until asynchronous `job` has finished"
  {:deprecated "Use wait!, wait-nil! or wait-result! instead"}
  [client access-token job]
  (wait-result! client access-token job)
  ::completed)

(defn resolve-jobs
  "Wait until all of the asynchronous `jobs` have finished"
  {:deprecated "Use wait-all! or wait-results! instead"}
  [client access-token jobs]
  (mapv (fn [job] (resolve-job client access-token job)) jobs))

(defn writes-locked? [client access-token]
  (-> client
      (i/request i/status-writes-locked access-token)
      (Boolean/parseBoolean)))

(defn with-job-timeout
  "Return a client with the speified job timeout set.  The job timeout will be raised"
  [client job-timeout]
  (->DrafterClient (:martian client)
                   (assoc (:opts client) :job-timeout job-timeout)
                   (:auth-provider client)
                   (:auth0 client)))

(defn client
  "Create a Drafter client for `drafter-uri` where the (web-)client will pass an
  access-token to each request."
  [drafter-uri & {:keys [batch-size version auth-provider auth0 job-timeout] :as opts}]
  (let [version (or version "v1")
        swagger-json "swagger/swagger.json"
        job-timeout (or job-timeout ##Inf)
        opts (-> opts (assoc :job-timeout job-timeout) (dissoc :auth0))]
    (log/debugf "Making Drafter client with batch size %d for Drafter: %s"
                batch-size drafter-uri)
    (when (seq drafter-uri)
      (-> (format "%s/%s" drafter-uri swagger-json)
          (martian-http/bootstrap-swagger {:interceptors i/default-interceptors})
          (->DrafterClient opts auth-provider auth0)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integrant ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/init-key :drafter-client/client
  [ig-key {:keys [drafter-uri] :as opts}]
  (when (seq drafter-uri)
    (let [opts (apply concat (dissoc opts :drafter-uri))]
      (try
        (println client opts)
        (apply client drafter-uri opts)
        (catch Throwable t
          (let [e (Throwable->map t)]
            (throw
             (ex-info (str "Failure to init " ig-key "\n"
                           (:cause e)
                           "\nCheck that Drafter is running!"
                          "\nCheck that your Drafter Client config is correct.")
                      e))))))))

(defmethod ig/halt-key! :drafter-client/client [_ client]
  ;; Shutdown client.
  ;; TODO Anything to do here?
  ;; TODO Will there be anything running in the background that we should wait
  ;; for?
  )

(s/def ::batch-size pos-int?)
;; TODO Find out if we can read this as a URI with integrant
(s/def ::drafter-uri (s/nilable string?))
(s/def ::version (s/nilable pos-int?))
(s/def ::auth0 auth/client?)

(defmethod ig/pre-init-spec :drafter-client/client [_]
  (s/nilable (s/keys :req-un [::batch-size ::drafter-uri]
                     :opt-un [::auth0 ::version])))

(defn- sync-body [fn-sym arg-list]
  `(let [job# (~fn-sym ~@arg-list)]
     (wait! ~(first arg-list) ~(second arg-list) job#)))

(defn- build-args [arg-list]
  (let [rest-index (.indexOf arg-list '&)]
    (if (not= rest-index -1)
      (->> rest-index
           (subvec arg-list)
           last
           :keys
           (mapcat (fn [key] [(keyword key) key]))
           (into (subvec arg-list 0 rest-index)))
      arg-list)))

(defmacro gensync
  "Macro which defines a synchronous version of the specified async client function. The async function
   should have at least two parameters where the first is the drafter client and the second the access
   token to use. The resulting function for an async function 'operation' is called 'operation-sync'"
  [fn-sym]
  (let [sync-fn (symbol (str fn-sym "-sync"))
        {:keys [arglists] :as fn-meta} (meta (ns-resolve *ns* fn-sym))
        doc (str "Synchronous version of " fn-sym " i.e. calls " fn-sym " and waits for the resulting job to complete")]
    (if (= 1 (count arglists))
      (let [arg-list (first arglists)
            args (build-args arg-list)]
        `(defn ~sync-fn ~doc ~arg-list ~(sync-body fn-sym args)))
      (let [arities (map (fn [arg-list]
                           (list arg-list (sync-body fn-sym (build-args arg-list))))
                         arglists)]
        `(defn ~sync-fn ~doc ~@arities)))))

(gensync remove-draftset)
(gensync load-graph)
(gensync delete-quads)
(gensync delete-triples)
(gensync add)
(gensync add-data)
(gensync publish)
