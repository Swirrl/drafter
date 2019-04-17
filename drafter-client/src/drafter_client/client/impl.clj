(ns drafter-client.client.impl
  (:refer-clojure :exclude [get])
  (:require [martian.core :as martian]
            [drafter-client.client.auth :as auth]))

(deftype DrafterClient [martian jws-key batch-size]
  ;; Wrap martian in a type so that:
  ;; a) we don't leak the jws-key
  ;; b) we don't expose the martian impl to the "system"
  ;; We can still get at the pieces if necessary due to the ILookup impl.
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :jws-key jws-key
      :batch-size batch-size
      (.valAt martian k default))))

(defn intercept
  {:style/indent :defn}
  [{:keys [martian jws-key batch-size] :as client} & interceptors]
  (->DrafterClient
    (apply update martian :interceptors conj interceptors) jws-key batch-size))

(defn claim-draftset
  "Claim this draftset as your own"
  #:drafter-client.client.impl{:generated true}
  [client id]
  (martian/response-for client :claim-draftset {:id id}))

(defn create-draftset
  "Create a new Draftset"
  #:drafter-client.client.impl{:generated true}
  [client & {:keys [display-name description] :as opts}]
  (martian/response-for client :create-draftset (merge {} opts)))

(defn delete-draftset
  "Delete the Draftset and its data"
  #:drafter-client.client.impl{:generated true}
  [client id]
  (martian/response-for client :delete-draftset {:id id}))

(defn delete-draftset-changes
  "Remove all the changes to a named graph from the Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id graph]
  (martian/response-for
   client
   :delete-draftset-changes
   {:id id :graph graph}))

(defn delete-draftset-data
  "Remove the supplied RDF data from this Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id data & {:keys [graph] :as opts}]
  (martian/response-for
   client
   :delete-draftset-data
   (merge {:id id :data data} opts)))

(defn delete-draftset-graph
  "Delete the contents of a graph in this Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id graph & {:keys [silent] :as opts}]
  (martian/response-for
   client
   :delete-draftset-graph
   (merge {:id id :graph graph} opts)))

(defn get-draftset
  "Get information about a Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id]
  (martian/response-for client :get-draftset {:id id}))

(defn get-draftset-data
  "Access the quads inside this Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id & {:keys [graph union-with-live timeout] :as opts}]
  (martian/response-for
   client
   :get-draftset-data
   (merge {:id id} opts)))

(defn get-draftsets
  "List available Draftsets"
  #:drafter-client.client.impl{:generated true}
  [client & {:keys [include] :as opts}]
  (martian/response-for client :get-draftsets (merge {} opts)))

(defn get-query-draftset
  "Query this Draftset with SPARQL"
  #:drafter-client.client.impl{:generated true}
  [client
   id
   query
   &
   {:keys
    [union-with-live timeout default-graph-uri named-graph-uri]
    :as opts}]
  (martian/response-for
   client
   :get-query-draftset
   (merge {:id id :query query} opts)))

(defn get-query-live
  "Queries the published data with SPARQL"
  #:drafter-client.client.impl{:generated true}
  [client
   query
   &
   {:keys [timeout default-graph-uri named-graph-uri] :as opts}]
  (martian/response-for
   client
   :get-query-live
   (merge {:query query} opts)))

(defn get-users
  "Gets all users"
  #:drafter-client.client.impl{:generated true}
  [client]
  (martian/response-for client :get-users {}))

(defn post-query-draftset
  "Query this Draftset with SPARQL"
  #:drafter-client.client.impl{:generated true}
  [client
   id
   query
   &
   {:keys
    [union-with-live timeout default-graph-uri named-graph-uri]
    :as opts}]
  (martian/response-for
   client
   :post-query-draftset
   (merge {:id id :query query} opts)))

(defn post-query-live
  "Queries the published data with SPARQL"
  #:drafter-client.client.impl{:generated true}
  [client
   query
   &
   {:keys [default-graph-uri named-graph-uri] :as opts}]
  (martian/response-for
   client
   :post-query-live
   (merge {:query query} opts)))

(defn publish-draftset
  "Publish the specified Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id]
  (martian/response-for client :publish-draftset {:id id}))

(defn put-draftset
  "Set metadata on Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id & {:keys [display-name description] :as opts}]
  (martian/response-for client :put-draftset (merge {:id id} opts)))

(defn put-draftset-data
  "Append the supplied RDF data to this Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id data & {:keys [graph] :as opts}]
  (martian/response-for
   client
   :put-draftset-data
   (merge {:id id :data data} opts)))

(defn put-draftset-graph
  "Copy a graph from live into this Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id graph]
  (martian/response-for
   client
   :put-draftset-graph
   {:id id :graph graph}))

(defn status-job-finished
  "Poll to see if asynchronous job has finished"
  #:drafter-client.client.impl{:generated true}
  [client jobid]
  (martian/response-for client :status-job-finished {:jobid jobid}))

(defn status-writes-locked
  "Poll to see if the system is accepting writes"
  #:drafter-client.client.impl{:generated true}
  [client]
  (martian/response-for client :status-writes-locked {}))

(defn submit-draftset-to
  "Submit a Draftset to a user or role"
  #:drafter-client.client.impl{:generated true}
  [client id & {:keys [role user] :as opts}]
  (martian/response-for
   client
   :submit-draftset-to
   (merge {:id id} opts)))

(defn get
  {:style/indent :defn}
  [client f user & args]
  (let [client (intercept client (auth/jws-auth client user))]
    (:body (apply f client args))))

(defn accept [client content-type]
  (intercept client
    {:name ::content-type
     :enter (fn [ctx]
              (assoc-in ctx [:request :headers "Accept"] content-type))}))

(defn content-type [client content-type]
  (intercept client
    {:name ::content-type
     :enter (fn [ctx]
              (assoc-in ctx [:request :headers "Content-Type"] content-type))}))
