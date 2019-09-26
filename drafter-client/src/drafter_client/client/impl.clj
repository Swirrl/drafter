(ns drafter-client.client.impl
  (:refer-clojure :exclude [get])
  (:require [cheshire.core :as json]
            [clojure.walk :refer [postwalk]]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.formats :refer [mimetype->rdf-format]]
            [grafter-2.rdf4j.io :as rio]
            [martian.clj-http :as martian-http]
            [martian.core :as martian]
            [martian.encoders :as encoders]
            [martian.interceptors :as interceptors]
            [ring.util.codec :refer [form-decode form-encode]]
            [ring.util.io :refer [piped-input-stream]]))

(deftype DrafterClient [martian batch-size auth0]
  ;; Wrap martian in a type so that:
  ;; a) we don't leak the auth0 client
  ;; b) we don't expose the martian impl to the "system"
  ;; We can still get at the pieces if necessary due to the ILookup impl.
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :batch-size batch-size
      :auth0 auth0
      (.valAt martian k default))))

(defn intercept
  {:style/indent :defn}
  [{:keys [martian batch-size auth0] :as client}
   & interceptors]
  (->DrafterClient (apply update martian :interceptors conj interceptors)
                   batch-size
                   auth0))

(defn bearer-token [access-token]
  (let [token (str "Bearer " access-token)]
    {:name ::bearer-token
     :enter #(assoc-in % [:request :headers "Authorization"] token)}))

(defn set-bearer-token [client access-token]
  (intercept client (bearer-token access-token)))

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

(defn get-job
  "Get metadata about a specific Job"
  #:drafter-client.client.impl{:generated true}
  [client jobid]
  (martian/response-for client :get-job {:jobid jobid}))

(defn get-jobs
  "Get a list of all jobs currently known"
  #:drafter-client.client.impl{:generated true}
  [client & opts]
  (martian/response-for client :get-jobs (merge {} opts)))

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
  [client f access-token & args]
  (if client
    (let [client (set-bearer-token client access-token)]
      (:body (apply f client args)))
    (throw (ex-info "Trying to make request to drafter with `nil` client."
                    {:type :no-drafter-client}))))

(defn accept [client content-type]
  (intercept client
    {:name ::content-type
     :enter (fn [ctx]
              (assoc-in ctx [:request :headers "Accept"] content-type))}))

(defn content-type [content-type]
  {:name ::content-type
   :enter (fn [ctx]
            (assoc-in ctx [:request :headers "Content-Type"] content-type))})

(defn set-content-type [client c-type]
  (intercept client (content-type c-type)))

(defn keywordize-keys
  "Recursively transforms all map keys from strings to keywords."
  [m]
  (let [f (fn [[k v]] (if (string? k) [(keyword k) v] [k v]))]
    (postwalk (fn [x]
                (cond (record? x)
                      (let [ks (filter string? (keys x))]
                        (into (apply dissoc x ks) (map f x)))
                      (map? x) (into {} (map f x))
                      :else x))
              m)))

(def keywordize-params
  {:name ::keywordize-params
   :enter (fn [ctx] (update ctx :params keywordize-keys))})

(defn set-redirect-strategy [strategy]
  {:name ::set-redirect-strategy
   :enter (fn [ctx] (assoc-in ctx [:request :redirect-strategy] strategy))})

(defn set-max-redirects [n]
  {:name ::set-max-redirects
   :enter (fn [ctx] (assoc-in ctx [:request :max-redirects] n))})

(def default-format
  {:date-format     "yyyy-MM-dd"
   :datetime-format "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"})

(defn- json [data]
  (let [opts {:date-format (:datetime-format default-format)}]
    (json/generate-string data opts)))

(defn- n*->stream [format n*]
  (piped-input-stream
   (fn [output-stream]
     (pr/add (rio/rdf-writer output-stream :format format) n*))))

(defn- grafter->format-stream [content-type data]
  (let [format (mimetype->rdf-format content-type)]
    (n*->stream format data)))

(defn- read-body [content-type body]
  {:pre [(instance? java.io.InputStream body)]}
  (let [rdf-format (mimetype->rdf-format content-type)]
    (rio/statements body :format rdf-format)))

(defn n-binary-encoder [content-type]
  {:encode (partial grafter->format-stream content-type)
   :decode (partial read-body content-type)
   :as :stream})

(def json-encoder
  {:encode json :decode #(encoders/json-decode % keyword)})

(def form-encoder
  {:encode form-encode :decode form-decode})

(def default-encoders
  (assoc (encoders/default-encoders)
         "application/x-www-form-urlencoded" form-encoder
         "application/json" json-encoder
         "application/n-quads" (n-binary-encoder "application/n-quads")
         "application/n-triples" (n-binary-encoder "application/n-triples")))

(def default-interceptors
  (vec (concat [keywordize-params]
               (rest martian/default-interceptors)
               [(interceptors/encode-body default-encoders)
                (interceptors/coerce-response default-encoders)
                martian-http/perform-request])))
