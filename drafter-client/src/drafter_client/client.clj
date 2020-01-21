(ns drafter-client.client
  (:refer-clojure :exclude [name type get])
  (:require [cheshire.core :as json]
            [clj-time.format :refer [formatters parse]]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [swirrl.auth0.client :as auth]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.impl :as i :refer [->DrafterClient]]
            [drafter-client.client.repo :as repo]
            [integrant.core :as ig]
            [martian.clj-http :as martian-http]
            [martian.core :as martian])
  (:import clojure.lang.ExceptionInfo
           java.util.UUID
           (java.io File)))

(alias 'c 'clojure.core)

(def live draftset/live)

(defn uuid [s]
  (some-> s UUID/fromString (try (catch Throwable _))))

(defn exception? [v]
  (instance? Exception v))

(s/def ::type #{"ok" "error" "not-found"})
(s/def :ok/type #{"ok"})
(s/def :error/type #{"error"})
(s/def :not-found/type #{"not-found"})
(s/def ::details map?)
(s/def ::job-id uuid?)
(s/def ::restart-id uuid?)
(s/def ::message string?)
(s/def ::error-class string?)

(s/def ::JobResponse (s/keys :req-un [::status ::job-id ::restart-id]))

(defmulti job-state-type (comp keyword :type))

(defmethod job-state-type :ok [_]
  (s/keys :req-un [::type] :opt-un [::details]))

(defmethod job-state-type :error [_]
  (s/keys :req-un [::type ::message ::error-class]))

(s/def ::NotFoundJobState (s/keys :req-un [:not-found/type ::restart-id]))

(defmethod job-state-type :not-found [_]
  (s/get-spec ::NotFoundJobState))

(s/def ::JobState (s/multi-spec job-state-type :type))

(defn date-time [s]
  (some->> s (parse (formatters :date-time))))

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
(s/def ::AsyncJob (s/and #(instance? AsyncJob %)
                         (s/keys :req-un [::job-id ::restart-id])))

(defn- ->async-job [{:keys [finished-job restart-id] :as rsp}]
  (let [job-id (-> finished-job (str/split #"/") last uuid)]
    (->AsyncJob job-id (java.util.UUID/fromString restart-id))))

(defn job-succeeded? [{:keys [type] :as job-state}]
  (= "ok" type))

(s/fdef job-succeeded? :args (s/cat :job-state ::JobState) :ret boolean?)

(defn job-failed? [{:keys [type] :as job-state}]
  (= "error" type))

(s/fdef job-failed? :args (s/cat :job-state ::JobState) :ret boolean?)

(defn job-complete? [job-state]
  (or (job-succeeded? job-state)
      (job-failed? job-state)))

(s/fdef job-complete? :args (s/cat :job-state ::JobState) :ret boolean?)

(defn job-in-progress? [{:keys [type] :as job-state}]
  (= "not-found" type))

(s/fdef job-in-progress? :args (s/cat :job-state ::JobState) :ret boolean?)

(defn drafter-restarted?
  "Whether drafter restarted between two polled states of a job."
  [job state]
  {:pre [(some? (:restart-id job))
         (some? (:restart-id state))]}
  (not= (:restart-id job) (:restart-id state)))

(s/fdef drafter-restarted? :args (s/cat :job ::AsyncJob :state ::NotFoundJobState) :ret boolean?)

(defn- job-details [job-state]
  (:details job-state))

(defn- json-draftset->draftset [ds]
  (let [{:keys [id display-name description]} ds
        id (uuid id)]
    (draftset/->draftset id display-name description)))

(defn- ->draftset [ds]
  (-> (draftset/map->Draftset ds)
      (update :id uuid)
      (update :created-at date-time)
      (update :updated-at date-time)
      (assoc :name (:display-name ds))
      (dissoc :display-name)))

(defn ->repo [client access-token context]
  (repo/make-repo client context access-token {}))

(defn draftsets
  "List available Draftsets"
  [client access-token]
  (->> (i/request client i/get-draftsets access-token)
       (map json-draftset->draftset)))

(defn new-draftset
  "Create a new Draftset"
  [client access-token name description]
  (-> client
      (i/request i/create-draftset access-token :display-name name :description description)
      (json-draftset->draftset)))

(defn get-draftsets
  "List available draftsets, optionally `:include` either
  `#{:all :owned :claimable}`.
  Returns all properties."
  [client access-token & [include]]
  (let [get-draftsets (partial i/request client i/get-draftsets access-token)
        include (if (keyword include) (c/name include) include)
        response (if include (get-draftsets :include include) (get-draftsets))]
    (map ->draftset response)))

(defn get-draftset [client access-token id]
  (->draftset (i/request client i/get-draftset access-token id)))

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
  [client access-token draftset & {:keys [metadata]}]
  (-> client
      (i/request i/delete-draftset access-token (draftset/id draftset) :metadata metadata)
      (->async-job)))

(defn load-graph
  "Load the graph from live into the Draftset"
  [client access-token draftset graph & {:keys [metadata]}]
  (-> client
      (i/request i/put-draftset-graph
                 access-token
                 (draftset/id draftset)
                 (str graph)
                 :metadata metadata)
      (->async-job)))

(defn delete-draftset-changes
  "Remove all changes from the named graph"
  [client access-token draftset graph]
  (i/request client i/delete-draftset-changes access-token
             (draftset/id draftset) (str graph)))

(defn delete-graph
  "Schedules the deletion of the graph from live"
  [client access-token draftset graph]
  (i/request client i/delete-draftset-graph access-token (draftset/id draftset) (str graph)))

(defn delete-quads
  [client access-token draftset quads & {:keys [metadata]}]
  (-> client
      (i/set-content-type "application/n-quads")
      (i/request i/delete-draftset-data access-token (draftset/id draftset) quads :metadata metadata)
      (->async-job)))

(defn delete-triples
  [client access-token draftset graph triples & {:keys [metadata]}]
  (-> client
      (i/set-content-type "application/n-triples")
      (i/request i/delete-draftset-data
                 access-token
                 (draftset/id draftset)
                 triples
                 :graph graph
                 :metadata metadata)
      (->async-job)))

(defn add-data
  "Append the supplied RDF statements to this Draftset.
  - `statements` can be a sequence of quads or triples, a File, or an InputStream
  - `opts` is a map of optional arguments, which may include:
    - `graph`: required if the statements are triples
    - `metadata`: a map with arbitrary keys that will be included on the job for future reference"
  [client access-token draftset statements & {:keys [graph metadata]}]
  (let [url (martian/url-for client
                             :put-draftset-data
                             {:id (draftset/id draftset)})
        format (i/get-format statements graph)]
    (-> (i/append-via-http-stream access-token
                                  url
                                  statements
                                  :graph graph
                                  :format format
                                  :metadata metadata)
        (->async-job))))

(defn add
  "Append the supplied RDF statements to this Draftset.
  'statements' can be a sequence of quads or triples; a File; or InputStream"
  {:deprecated "Use add-data instead"}
  ([client access-token draftset quads]
   (add-data client access-token draftset quads))
  ([client access-token draftset graph triples]
   (add-data client access-token draftset triples :graph graph)))

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
  [client access-token draftset & {:keys [metadata]}]
  (-> client
      (i/request i/publish-draftset access-token (draftset/id draftset) :metadata metadata)
      (->async-job)))

(defn get
  "Access the quads inside this Draftset"
  ([client access-token draftset]
   (-> client
       (i/accept "application/n-quads")
       (i/request i/get-draftset-data access-token (draftset/id draftset))))
  ([client access-token draftset graph]
   (-> client
       (i/accept "application/n-triples")
       (i/request i/get-draftset-data access-token (draftset/id draftset) :graph graph))))

(defn job [client access-token id]
  (->job (i/request client i/get-job access-token id)))

(defn jobs [client access-token]
  (map ->job (i/request client i/get-jobs access-token)))

(defn- parse-not-found-body
  "Parses the HTTP body from a not-found job state response"
  [body]
  (let [m (json/parse-string body keyword)]
    (update m :restart-id uuid)))

(s/fdef parse-not-found-body :args (s/cat :body string?) :ret ::NotFoundJobState)

(defn- refresh-job
  "Poll to get the latest state of a job"
  [client access-token {:keys [job-id] :as job}]
  (try
    (i/request client i/status-job-finished access-token job-id)
    (catch ExceptionInfo e
      (let [{:keys [body status]} (ex-data e)]
        (if (= status 404)
          (parse-not-found-body body)
          (throw e))))))

(s/fdef refresh-job
  :args (s/cat :client any? :access-token any? :job ::AsyncJob)
  :ret ::JobState)

(s/def ::JobSucceededResult (s/nilable map?))
(s/def ::JobFailedResult exception?)
(s/def ::JobResult (s/or :succeeded ::JobSucceededResult :failed ::JobFailedResult))

(def job-failure-result? exception?)

(defn job-status
  "Returns a value representing the result of the given async job or ::pending if the
   job is still in progress."
  [job job-state]
  (let [info {:job job :state job-state}]
    (cond
      (job-succeeded? job-state) (job-details job-state)
      (job-failed? job-state) (ex-info "Job failed" info)
      (drafter-restarted? job job-state) (ex-info "Drafter restarted while waiting for job" info)
      (job-in-progress? job-state) ::pending
      :else (ex-info "Unknown job state" info))))

(s/fdef job-status
  :args (s/cat :job ::AsyncJob :job-state ::JobState)
  :ret (s/or :result ::JobResult :pending #{::pending}))

(defn job-timeout-exception [job]
  (ex-info "Timed out waiting for job to finish" {:type ::job-timeout :job job}))

(defn job-timeout-exception? [e]
  (-> e ex-data :type (= ::job-timeout)))

(defn wait-result!
  "Waits for an async job to complete and returns the result map if it succeeded
   or, returns an exception representing the failure otherwise."
  [client access-token job]
  (loop [waited 0]
    (let [state (refresh-job client access-token job)
          status (job-status job state)
          wait 500]
      (cond (>= waited (:job-timeout client))
            (job-timeout-exception job)
            (= ::pending status)
            (do (Thread/sleep wait) (recur (+ waited wait)))
            :else
            status))))

(s/fdef wait-result!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob)
  :ret ::JobResult)

(defn wait-results!
  "Waits for a sequence of jobs to complete and returns a sequence of results in corresponding order.
   If a job succeeded, the result will be a result map, otherwise an exception indiciating the reason
   for the failure."
  [client access-token jobs]
  (mapv #(wait-result! client access-token %) jobs))

(s/fdef wait-results!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :jobs (s/coll-of ::AsyncJob))
  :ret (s/coll-of ::JobResult))

(defn wait!
  "Waits for the specified job to complete. Returns the result of the job if successful or
  throws an exception if the job failed"
  [client access-token job]
  (let [result (wait-result! client access-token job)]
    (if (exception? result)
      (throw result)
      result)))

(s/fdef wait!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob)
  :ret ::JobSucceededResult)

(defn wait-nil!
  "Waits for a job to complete and returns nil if successful, otherwise throws an exception"
  [client access-token job]
  (wait! client access-token job)
  nil)

(s/fdef wait-nil!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob)
  :ret nil?)

(defn wait-all!
  "Waits for the specified jobs to complete. Returns a sequence of the complete job results in the
   corresponding order of the jobs in the source sequence if they all completed successfully, or
   throws an exception if any of the jobs failed."
  [client access-token jobs]
  (let [results (wait-results! client access-token jobs)
        failures (filter exception? results)
        succeeded (remove exception? results)]
    (if (seq failures)
      (throw (ex-info "One or more jobs failed" {:failed    failures
                                                 :succeeded succeeded}))
      succeeded)))

(s/fdef wait-all!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :jobs (s/coll-of ::AsyncJob))
  :ret (s/coll-of ::JobSucceededResult)
  :fn (fn [{ret :ret {jobs :jobs} :args}] (= (count ret) (count jobs))))

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

(defn with-job-timeout [client job-timeout]
  (->DrafterClient (:martian client)
                   (assoc (:opts client) :job-timeout job-timeout)
                   (:auth0 client)))

(defn client
  "Create a Drafter client for `drafter-uri` where the (web-)client will pass an
  access-token to each request."
  [drafter-uri & {:keys [batch-size version auth0 job-timeout] :as opts}]
  (let [version (or version "v1")
        swagger-json "swagger/swagger.json"
        job-timeout (or job-timeout ##Inf)
        opts (-> opts (assoc :job-timeout job-timeout) (dissoc :auth0))]
    (log/debugf "Making Drafter client with batch size %d for Drafter: %s"
                batch-size drafter-uri)
    (when (seq drafter-uri)
      (-> (format "%s/%s" drafter-uri swagger-json)
          (martian-http/bootstrap-swagger {:interceptors i/default-interceptors})
          (->DrafterClient opts auth0)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integrant ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/init-key :drafter-client/client
  [ig-key {:keys [drafter-uri] :as opts}]
  (when (seq drafter-uri)
    (let [opts (apply concat (dissoc opts :drafter-uri))]
      (try
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
