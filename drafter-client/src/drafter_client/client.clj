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
            [martian.clj-http :as martian-http])
  (:import clojure.lang.ExceptionInfo
           java.util.UUID))

(alias 'c 'clojure.core)

(def live draftset/live)

(defn uuid [s]
  (some-> s UUID/fromString (try (catch Throwable _))))

(defn exception? [v]
  (instance? Exception v))

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

(defn- ->async-job [{:keys [finished-job restart-id] :as rsp}]
  (let [job-id (-> finished-job (str/split #"/") last uuid)]
    (->AsyncJob job-id (java.util.UUID/fromString restart-id))))

(defn job-succeeded? [{:keys [type] :as job-state}]
  (= "ok" type))

(defn job-failed? [{:keys [type] :as job-state}]
  (= "error" type))

(defn job-complete? [job-state]
  (or (job-succeeded? job-state)
      (job-failed? job-state)))

(defn job-in-progress? [{:keys [type] :as job-state}]
  (= "not-found" type))

(defn drafter-restarted?
  "Whether drafter restarted between two polled states of a job."
  [job state]
  {:pre [(some? (:restart-id job))
         (some? (:restart-id state))]}
  (not= (:restart-id job) (:restart-id state)))

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
  (->> (i/get client i/get-draftsets access-token)
       (map json-draftset->draftset)))

(defn new-draftset
  "Create a new Draftset"
  [client access-token name description]
  (-> client
      (i/get i/create-draftset access-token :display-name name :description description)
      (json-draftset->draftset)))

(defn get-draftsets
  "List available draftsets, optionally `:include` either
  `#{:all :owned :claimable}`.
  Returns all propeties."
  [client access-token & [include]]
  (let [include (if (keyword include) (c/name include) include)]
    (->> (i/get client i/get-draftsets access-token :include include)
         (map ->draftset))))

(defn get-draftset [client access-token id]
  (->draftset (i/get client i/get-draftset access-token id)))

(defn edit-draftset [client access-token id name description]
  (i/get client i/put-draftset access-token id
         :display-name name
         :description description))

(defn submit-to-user [client access-token id user]
  (i/get client i/submit-draftset-to access-token id :user user))

(defn submit-to-role [client access-token id role]
  (let [role (if (keyword? role) (c/name role) role)]
    (i/get client i/submit-draftset-to access-token id :role role)))

(defn claim [client access-token id]
  (i/get client i/claim-draftset access-token id))

(defn remove-draftset
  "Delete the Draftset and its data"
  [client access-token draftset]
  (-> client
      (i/get i/delete-draftset access-token (draftset/id draftset))
      (->async-job)))

(defn load-graph
  "Load the graph from live into the Draftset"
  [client access-token draftset graph]
  (-> client
      (i/get i/put-draftset-graph access-token (draftset/id draftset) (str graph))
      (->async-job)))

(defn delete-graph
  "Schedules the deletion of the graph from live"
  [client access-token draftset graph]
  (i/get client i/delete-draftset-graph access-token (draftset/id draftset) (str graph)))

(defn delete-quads
  [client access-token draftset quads]
  (-> client
      (i/set-content-type "application/n-quads")
      (i/get i/delete-draftset-data access-token (draftset/id draftset) quads)
      (->async-job)))

(defn delete-triples
  [client access-token draftset graph triples]
  (-> client
      (i/set-content-type "application/n-triples")
      (i/get i/delete-draftset-data access-token (draftset/id draftset) triples :graph graph)
      (->async-job)))

(defn add
  "Append the supplied RDF data to this Draftset"
  ([client access-token draftset quads]
   (-> client
       (i/set-content-type "application/n-quads")
       (i/get i/put-draftset-data access-token (draftset/id draftset) quads)
       (->async-job)))
  ([client access-token draftset graph triples]
   (let [id (draftset/id draftset)]
     (-> client
         (i/set-content-type "application/n-triples")
         (i/get i/put-draftset-data access-token id triples :graph graph)
         (->async-job)))))

(defn add-in-batches
  "Append the supplied RDF data to this Draftset in batches"
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
  [client access-token draftset]
  (-> client
      (i/get i/publish-draftset access-token (draftset/id draftset))
      (->async-job)))

(defn get
  "Access the quads inside this Draftset"
  ([client access-token draftset]
   (-> client
       (i/accept "application/n-quads")
       (i/get i/get-draftset-data access-token (draftset/id draftset))))
  ([client access-token draftset graph]
   (-> client
       (i/accept "application/n-triples")
       (i/get i/get-draftset-data access-token (draftset/id draftset) :graph graph))))


(defn job [client access-token id]
  (->job (i/get client i/get-job access-token id)))

(defn jobs [client access-token]
  (map ->job (i/get client i/get-jobs access-token)))

(defn- parse-not-found-body
  "Parses the HTTP body from a not-found job state response"
  [body]
  (let [m (json/parse-string body keyword)]
    (update m :restart-id uuid)))

(defn- refresh-job
  "Poll to see if asynchronous job has finished"
  [client access-token {:keys [job-id] :as job}]
  {:pre [(some? job-id)]}
  (try
    (i/get client i/status-job-finished access-token job-id)
    (catch ExceptionInfo e
      (let [{:keys [body status]} (ex-data e)]
        (if (= status 404)
          (parse-not-found-body body)
          (throw e))))))

(def job-failure-result? exception?)

(defn job-status [job job-state]
  (let [info {:job job :state job-state}]
    (cond
      (job-succeeded? job-state) (job-details job-state)
      (job-failed? job-state) (ex-info "Job failed" info)
      (drafter-restarted? job job-state) (ex-info "Drafter restarted while waiting for job" info)
      (job-in-progress? job-state) ::pending
      :else (ex-info "Unknown job state" info))))

(defn wait-result!
  "Waits for an async job to complete and returns the result map if it succeeded
   or an exception representing the failure otherwise."
  [client access-token job]
  (let [state (refresh-job client access-token job)
        status (job-status job state)]
    (if (= ::pending status)
      (do (Thread/sleep 500)
          (recur client access-token job))
      status)))

(defn wait-results!
  "Waits for a sequence of jobs to complete and returns a sequence of results in corresponding order.
   If a job succeeded, the result will be a result map, otherwise an exception indiciating the reason
   for the failure."
  [client access-token jobs]
  (mapv #(wait-result! client access-token %) jobs))

(defn wait!
  "Waits for the specified job to complete. Returns the result of the job if successful or
  throws an exception if the job failed"
  [client access-token job]
  (let [result (wait-result! client access-token job)]
    (if (exception? result)
      (throw result)
      result)))

(defn wait-nil!
  "Waits for a job to complete and returns nil if successful, otherwise throws an exception"
  [client access-token job]
  (wait! client access-token job)
  nil)

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

(defn client
  "Create a Drafter client for `drafter-uri` where the (web-)client will pass an
  access-token to each request."
  [drafter-uri & {:keys [batch-size version auth0]}]
  (let [version (or version "v1")
        swagger-json "swagger/swagger.json"]
    (log/debugf "Making Drafter client with batch size %d for Drafter: %s"
                batch-size drafter-uri)
    (when (seq drafter-uri)
      (-> (format "%s/%s" drafter-uri swagger-json)
          (martian-http/bootstrap-swagger {:interceptors i/default-interceptors})
          (->DrafterClient batch-size auth0)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integrant ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/init-key :drafter-client/client
  [ig-key {:keys [drafter-uri batch-size version auth0]}]
  (when (seq drafter-uri)
    (try
      (client drafter-uri :batch-size batch-size :version version :auth0 auth0)
      (catch Throwable t
        (let [e (Throwable->map t)]
          (throw
           (ex-info (str "Failure to init " ig-key "\n"
                         (:cause e)
                         "\nCheck that Drafter is running!"
                         "\nCheck that your Drafter Client config is correct.")
                    e)))))))

(defmethod ig/halt-key! :drafter-client/client [_ client]
  ;; Shutdown client.
  ;; TODO Anything to do here?
  ;; TOOD Will there be anything running in the background that we should wait
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
