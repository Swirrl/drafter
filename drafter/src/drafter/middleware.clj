(ns drafter.middleware
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [drafter.rdf.content-negotiation :as conneg]
            [drafter.requests :as drafter-request]
            [drafter.responses :as response]
            [drafter.endpoint :as ep]
            [drafter.user :as user]
            [drafter.util :as util]
            [grafter-2.rdf4j.formats :refer [mimetype->rdf-format]]
            [ring.util.request :as request]
            [integrant.core :as ig]
            [buddy.auth.http :as http])
  (:import java.io.File
           (org.apache.tika.mime MediaType)
           (java.util.zip GZIPInputStream)))

(defn- whitelisted? [{:keys [request-method uri]}]
  (case request-method
    ;; "a CORS-preflight request never includes credentials"
    ;; https://fetch.spec.whatwg.org/#cors-protocol-and-credentials
    :options true
    ;; Allow access to swagger docs
    :get (#{"/" "/swagger/swagger.json"} uri)
    false))

(defmethod ig/init-key :drafter.middleware/wrap-authenticate
  [_ {:keys [middleware] :as opts}]
  (fn [handler]
    (let [wrapped ((apply comp middleware) handler)]
      (fn [request]
        (if (or (:identity request) ; already authenticated
                (whitelisted? request))
          (handler request)
          (wrapped request))))))

(defn wrap-authorize [wrap-authenticate required-role handler]
  (wrap-authenticate
   (fn [request]
     (if (whitelisted? request)
       (handler request)
       (let [user (:identity request)]
         (if (user/has-role? (:identity request) required-role)
           (handler request)
           (response/forbidden-response
            (str "You require the "
                 (name required-role)
                 " role to perform this action"))))))))

(defn require-params [required-keys inner-handler]
  (fn [{:keys [params] :as request}]
    (if-let [missing-keys (seq (set/difference required-keys (set (keys params))))]
      (response/unprocessable-entity-response (str "Missing required parameters: " (string/join "," (map name missing-keys))))
      (inner-handler request))))

(defn optional-enum-param
  "Wraps a handler in one which finds an optional parameter in the
  incoming request and ensures it is a string representation of one of
  the given valid keyword values. If the value is valid, the incoming
  string value is replace in the request with the keyword and the
  inner handler invoked with the modified request. If the value is not
  valid, a 422 Unprocessable Entity response is returned. If the
  parameter is not present in the request, the given default is
  associated with the request and the inner handler invoked."
  [param-name allowed-values default inner-handler]
  (fn [{:keys [params] :as request}]
    (letfn [(invoke-inner [param-val]
              (inner-handler (assoc-in request [:params param-name] param-val)))]
      (if-let [val (get params param-name)]
        (let [kw-val (keyword val)]
          (if (contains? allowed-values kw-val)
            (invoke-inner kw-val)
            (response/unprocessable-entity-response (str "Invalid value for parameter " (name param-name) ": " val))))
        (invoke-inner default)))))

(defn include-endpoints-param
  [inner-handler]
  (optional-enum-param
    :include ep/includes :all
    inner-handler))

(defn media-type-named
  [^String name]
  (MediaType/parse name))

(defn require-content-type
  "Wraps a ring handler in one which requires the incoming request has
  a valid content type. If no content type is present, or if it is
  malformed, a 422 Unprocessable Entity response is returned."
  [inner-handler]
  (fn [request]
    (if-let [content-type (request/content-type request)]
      (let [media-type (media-type-named content-type)]
        (if (nil? media-type)
          (response/unprocessable-entity-response (str "Invalid content type: " content-type))
          (inner-handler request)))
      (response/unprocessable-entity-response "Content type required"))))

(defn require-rdf-content-type
  "Wraps a ring handler in one which requires the incoming request has
  a content type which maps to a known serialisation format for
  RDF. If the content type does not exist or is invalid, a 422
  Unprocessable Entity response is returned. If the content type does
  not map to a known RDF format, a 415 Unsupported Media Type response
  is returned. Otherwise the inner handler will be invoked with a
  request containing two extra keys in the request parameters:
    - rdf-format A sesame RDFFormat instance for the format
    - rdf-content-type The content type for the request"
  [inner-handler]
  (require-content-type
   (fn [request]
     (let [content-type (request/content-type request)]
       (if-let [rdf-format (mimetype->rdf-format content-type)]
         (let [modified-request (util/merge-in request [:params] {:rdf-format rdf-format
                                                                  :rdf-content-type content-type})]
           (inner-handler modified-request))
         (response/unsupported-media-type-response  (str "Unsupported media type: " content-type)))))))

(defn- header-matches? [^String candidate ^String header]
  (.equalsIgnoreCase candidate header))

(defn- is-gzipped-entity?
  "Whether the input request has a GZIP-compressed request body"
  [{:keys [body headers] :as request}]
  (let [encoding (get headers "content-encoding")]
    (and (some? body)
         (some? encoding)
         (boolean (some #(header-matches? % encoding) ["gzip" "x-gzip"])))))

(defn- inflate-gzipped-body
  [request]
  (if (is-gzipped-entity? request)
    (update request :body #(GZIPInputStream. (io/input-stream %)))
    request))

(defn inflate-gzipped
  "Wraps a handler with one that replaces the request body with an inflating input
  stream for the GZIP-compressed entity on the incoming request. Uncompressed bodies
  are not modified."
  [inner-handler]
  (fn [request]
    (inner-handler (inflate-gzipped-body request))))

(defn temp-file-body
  "Wraps a handler with one that writes the incoming body to a temp
  file and sets the new body as the file before invoking the inner
  handler."
  [inner-handler]
  (fn [{:keys [body] :as request}]
    (let [temp-file (File/createTempFile "drafter-body" nil)]
      (io/copy body temp-file)
      (inner-handler (assoc request :body temp-file)))))


(defn negotiate-sparql-results-content-type-with
  "Returns a handler which performs content negotiation for a SPARQL query with the given negotiation function.
   The negotitation function should return a pair [rdf-format response-content-type] if negotiation succeeds, or nil
   on failure. If negotiation fails a 406 Not Acceptable response is returned with the given message."
  [negotiate-f not-acceptable-message inner-handler]
  (fn [request]
    (let [accept (drafter-request/accept request)]
      (if-let [[rdf-format response-content-type] (negotiate-f accept)]
        (let [to-assoc {:format rdf-format
                        :response-content-type response-content-type}
              updated-request (update request :sparql #(merge % to-assoc))]
          (inner-handler updated-request))
        (response/not-acceptable-response not-acceptable-message)))))

(defn negotiate-quads-content-type-handler
  "Returns a handler which negotiates an RDF quads content type for SPARQL query responses."
  [inner-handler]
  (negotiate-sparql-results-content-type-with conneg/negotiate-rdf-quads-format "RDF quads format required" inner-handler))

(defn negotiate-triples-content-type-handler
  "Returns a handler which negotiates an RDF triples content type for SPARQL query responses."
  [inner-handler]
  (negotiate-sparql-results-content-type-with conneg/negotiate-rdf-triples-format "RDF triples format required" inner-handler))

(defn wrap-total-requests-counter [handler]
  (fn [req]
    (datadog/increment! "drafter.requests.total" 1)
    (handler req)))

(defn wrap-request-timer [handler]
  (fn [req]
    (datadog/measure!
     "drafter.request.time" {}
     (handler req))))
