(ns drafter-client.client.impl
  (:require [cheshire.core :as json]
            [drafter-client.auth.legacy-default :as default-auth]
            [drafter-client.client.interceptors :as interceptor]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.formats :refer [mimetype->rdf-format
                                             filename->rdf-format]]
            [grafter-2.rdf4j.io :as rio]
            [clj-http.client :as http]
            [martian.core :as martian]
            [martian.interceptors :as interceptors]
            [ring.util.codec :refer [form-decode form-encode]]
            [martian.encoders :as enc]
            [drafter-client.client.protocols :as dcpr]
            [clojure.java.io :as io])
  (:import (java.io InputStream File PipedInputStream PipedOutputStream)
           [java.util.zip GZIPInputStream GZIPOutputStream]))

(def ^{:deprecated "Use drafter-client.client.protocols/->DrafterClient instead"}
  ->DrafterClient dcpr/->DrafterClient)

(def ^{:deprecated "moved to drafter-client.client.interceptors/intercept"} intercept interceptor/intercept)

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
  [client id {:keys [metadata]}]
  (martian/response-for client
                        :delete-draftset
                        (cond-> {:id id}
                                metadata (merge {:metadata (enc/json-encode metadata)}))))

(defn delete-draftset-changes
  "Remove all the changes to a named graph from the Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id graph]
  (martian/response-for
   client
   :delete-draftset-changes
   {:id id :graph graph}))

(defn delete-draftset-data
  "Remove the supplied RDF data from this Draftset. Opts may include `graph` if the statements
  to be deleted are triples and `metadata`, which will be stored on the job for future reference."
  [client id data {:keys [metadata] :as opts}]
  (martian/response-for
   client
   :delete-draftset-data
   (cond-> (merge opts {:id id :data data})
           metadata (merge {:metadata (enc/json-encode metadata)}))))

(defn delete-draftset-graph
  "Delete the contents of a graph in this Draftset"
  [client id graph & {:keys [silent metadata] :as opts}]
  (martian/response-for
   client
   :delete-draftset-graph
   (cond-> (merge opts {:id id :graph graph})
     metadata (merge {:metadata (enc/json-encode metadata)}))))

(defn get-draftset
  "Get information about a Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id opts]
  (martian/response-for client :get-draftset (merge {:id id} opts)))

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
  [client {:keys [union-with-live include] :as opts}]
  (martian/response-for client :get-draftsets (merge {} opts)))

(defn get-endpoints
  "List available Endpoints"
  [client & {:keys [include] :as opts}]
  (martian/response-for client :get-endpoints (merge {} opts)))

(defn get-public-endpoint
  [client]
  (-> (martian/response-for client :get-public-endpoint)
      (:body)))

(defn create-public-endpoint
  [client opts]
  (martian/response-for client :create-public-endpoint opts))

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

(defn post-update-draftset
  "Update this Draftset with SPARQL"
  [client id update
   & {:keys [timeout using-graph-uri using-named-graph-uri] :as opts}]
  (martian/response-for client
                        :post-update-draftset
                        (merge {:id id :update update} opts)))

(defn publish-draftset
  "Publish the specified Draftset"
  [client id {:keys [metadata]}]
  (martian/response-for client
                        :publish-draftset
                        (cond-> {:id id}
                                metadata (merge {:metadata (enc/json-encode metadata)}))))

(defn put-draftset
  "Set metadata on Draftset"
  #:drafter-client.client.impl{:generated true}
  [client id & {:keys [display-name description] :as opts}]
  (martian/response-for client :put-draftset (merge {:id id} opts)))

(defn piped-input-stream [func]
  (let [input  (PipedInputStream.)
        output (PipedOutputStream.)
        _      (.connect input output)
        worker (future (try (func output) (finally (.close output))))]
    {:input input
     :worker worker}))

(defn- n*-output-stream-writer
  "Returns a function which writes the given data to its argument output stream in the given
   RDF format"
  [format data]
  (fn [output-stream]
    (pr/add (rio/rdf-writer output-stream :format format) data)
    (.flush output-stream)))

(defn grafter->format-stream [content-type data]
  (let [format (mimetype->rdf-format content-type)]
    (piped-input-stream
      (n*-output-stream-writer format data))))

(defn- legacy-auth-call? [access-token]
  (some? access-token))

(defn- pipe-gzipped [data]
  (piped-input-stream (fn [output-stream]
                            (with-open [os (GZIPOutputStream. output-stream)]
                              (io/copy data os)))))

(defn quads-write-f [data format gzip]
  (let [rdf-format (mimetype->rdf-format format)
        write-f (n*-output-stream-writer rdf-format data)]
    (if (true? gzip)
      (fn [output-stream]
        (with-open [gos (GZIPOutputStream. output-stream)]
          (write-f gos)))
      write-f)))

(defn format-body [data format gzip]
  (cond
    (instance? File data)
    (if (true? gzip)
      (pipe-gzipped data)
      {:input data})

    (instance? InputStream data)
    (if
      (true? gzip)
      (pipe-gzipped data)
      {:input data})

    :else
    (let [write-f (quads-write-f data format gzip)]
      (piped-input-stream write-f))))

(defn append-via-http-stream
  "Write statements (quads, triples, File, InputStream) to a URL, as a
  an input stream by virtue of an HTTP PUT"
  [access-token url statements {:keys [auth-provider graph format metadata gzip] :as opts}]
  (let [{:keys [input worker owned]} (format-body statements format gzip)
        headers {:Content-Type format
                 :Accept "application/json"
                 :Authorization (if (legacy-auth-call? access-token)
                                  (str "Bearer " access-token)
                                  (dcpr/authorization-header auth-provider))}
        headers (cond-> headers
                  gzip (assoc :Content-Encoding "gzip"))
        params (cond-> nil
                 graph (merge {:graph (.toString graph)})
                 metadata (merge {:metadata (enc/json-encode metadata)}))
        request {:url url
                 :query-params params
                 :method :put
                 :body input
                 :headers headers
                 :as :json}
        {:keys [body] :as _resp} (http/request request)]
    (when worker @worker)
    (when owned
      (.close input))
    
    body))

(defn put-draftset-graph
  "Copy a graph from live into this Draftset"
  [client id graph {:keys [metadata]}]
  (martian/response-for
   client
   :put-draftset-graph
   (cond-> {:id id :graph graph}
           metadata (merge {:metadata (enc/json-encode metadata)}))))

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

(defn- assert-client [client]
  (when-not client
    (throw (ex-info "Trying to make request to drafter with `nil` client."
                    {:type :no-drafter-client}))))

(defn with-authorization [{:keys [auth-provider] :as client} access-token]
  (cond
    (some? auth-provider)
    (interceptor/intercept client (dcpr/interceptor auth-provider))
    (some? access-token)
    (default-auth/with-auth0-default client access-token)
    :else client ;; at some point we could perhaps raise an error here...
    ))

(defn request-operation [client operation access-token opts]
  (assert-client client)
  (let [client (with-authorization client access-token)
        response (martian/response-for client operation opts)]
    (:body response)))

(defn request
  {:style/indent :defn}
  [client f access-token & args]
  (assert-client client)
  (let [client (with-authorization client access-token)]
    (:body (apply f client args))))


(def default-format
  {:date-format     "yyyy-MM-dd"
   :datetime-format "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"})

(defn- json [data]
  (let [opts {:date-format (:datetime-format default-format)}]
    (json/generate-string data opts)))

(defn- read-body [content-type body]
  {:pre [(instance? java.io.InputStream body)]}
  (let [rdf-format (mimetype->rdf-format content-type)]
    (rio/statements body :format rdf-format)))

(defn n-binary-encoder [content-type]
  {:encode (partial grafter->format-stream content-type)
   :decode (partial read-body content-type)
   :as :stream})

(def json-encoder
  {:encode json :decode #(enc/json-decode % keyword)})

(def form-encoder
  {:encode form-encode :decode form-decode})

(def default-encoders
  (assoc (enc/default-encoders)
         "application/x-www-form-urlencoded" form-encoder
         "application/json" json-encoder
         "application/n-quads" (n-binary-encoder "application/n-quads")
         "application/n-triples" (n-binary-encoder "application/n-triples")))

(def perform-request
  {:name ::perform-request
   :leave (fn [{:keys [request] :as ctx}]
            (let [{input-stream :input worker :worker} (:body request)
                  response (if input-stream
                             (-> (assoc request :body input-stream) http/request)
                             (http/request request))]
              (when worker @worker)
              (assoc ctx :response response)))})

(def default-interceptors
  (vec (concat [interceptor/keywordize-params]
               (rest martian/default-interceptors)
               [(interceptors/encode-body default-encoders)
                (interceptors/coerce-response default-encoders)
                perform-request])))

(defn get-format [statements graph]
  (or (when (instance? File statements)
        (some-> (.getPath statements)
                (filename->rdf-format)
                (.getDefaultMIMEType)))
      (when graph "application/n-triples")
      "application/n-quads"))
