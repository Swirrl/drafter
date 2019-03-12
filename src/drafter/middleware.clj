(ns drafter.middleware
  (:require [buddy.auth :as auth]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.backends.token :refer [jws-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
            [buddy.auth.protocols :as authproto]
            [clj-logging-config.log4j :as l4j]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [drafter.rdf.content-negotiation :as conneg]
            [drafter.rdf.sesame :as ses]
            [drafter.requests :as drafter-request]
            [drafter.responses :as response]
            [drafter.user :as user]
            [drafter.util :as util]
            [grafter.rdf4j.formats :refer [mimetype->rdf-format]]
            [grafter.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [pantomime.media :refer [media-type-named]]
            [ring.util.request :as request])
  (:import clojure.lang.ExceptionInfo
           java.io.File))

(defn- authenticate-user [user-repo request {:keys [username password] :as auth-data}]
  (log/info "auth user" username password)
  (if-let [user (user/find-user-by-username user-repo username)]
    (user/try-authenticate user password)))

(defn- basic-auth-backend [{:as user-repo :keys [realm]}]
  (let [conf {:realm (or realm "Drafter")
              :authfn #(authenticate-user user-repo %1 %2)
              :unauthorized-handler (fn [req err]
                                      (response/unauthorised-basic-response realm))}]
    (http-basic-backend conf)))

(defn- jws-auth-backend [token-auth-key]
  (let [conf {:secret token-auth-key
              :token-name "Token"
              :options {:alg :hs256
                        :iss "publishmydata"
                        :aud "drafter"}}
        inner-backend (jws-backend conf)]
    (reify authproto/IAuthentication
      (-parse [_ request] (authproto/-parse inner-backend request))
      (-authenticate [_ request data]
        (when-let [token (authproto/-authenticate inner-backend request data)]
          (try
            (user/validate-token! token)
            (catch ExceptionInfo ex
              (log/error ex "Token authentication failed due to an invalid user token")
              (auth/throw-unauthorized {:message "Invalid token"}))))))))

(defn require-authenticated
  "Requires the incoming request has been authenticated."
  [inner-handler]
  (fn [request]
    (if (auth/authenticated? request)
      (let [email (:email (:identity request))]
        (l4j/with-logging-context {:user email} ;; wrap a logging context over the request so we can trace the user
          (datadog/increment! "drafter.requests.authorised" 1)
          (log/info "got user" email)
          (inner-handler request)))
      (do
        (datadog/increment! "drafter.requests.unauthorised" 1)
        (auth/throw-unauthorized {:message "Authentication required"})))))

(defn- get-configured-token-auth-backend [config]
  (if-let [signing-key (:jws-signing-key config)]
    (jws-auth-backend signing-key)
    (do
      (log/warn "No JWS Token signing key configured - token authentication will not be available")
      (log/warn "To configure JWS Token authentication, specify the jws-signing-key configuration setting")
      nil)))

(defn- get-configured-auth-backends [user-repo config]
  (let [basic-backend (basic-auth-backend user-repo)
        jws-backend (get-configured-token-auth-backend config)]
    (remove nil? [basic-backend jws-backend])))

(defn- wrap-authenticated [auth-backends inner-handler realm]
  (let [auth-handler (apply wrap-authentication (require-authenticated inner-handler) auth-backends)
        unauthorised-fn (fn [req err]
                          (response/unauthorised-basic-response (or realm "Drafter")))]
    (wrap-authorization auth-handler unauthorised-fn)))

(defn make-authenticated-wrapper [{:keys [realm] :as user-repo} config]
  (let [auth-backends (get-configured-auth-backends user-repo config)]
    (fn [inner-handler]
      (wrap-authenticated auth-backends inner-handler realm))))

(defmethod ig/pre-init-spec :drafter.middleware/wrap-auth [_]
  (s/keys :req [::user/repo]))

(defmethod ig/init-key :drafter.middleware/wrap-auth [_ {:keys [::user/repo] :as config}]
  (make-authenticated-wrapper repo config))

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

