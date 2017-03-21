(ns drafter.middleware
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [drafter.util :as util]
            [drafter.responses :as response]
            [drafter.user :as user]
            [drafter.user.repository :as user-repo]
            [drafter.rdf.sesame :refer [read-statements]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [buddy.auth :as auth]
            [buddy.auth.protocols :as authproto]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.backends.token :refer [jws-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
            [ring.util.request :as request]
            [drafter.requests :as drafter-request]
            [pantomime.media :refer [media-type-named]]
            [drafter.rdf.content-negotiation :as conneg]
            [drafter.rdf.rewriting.arq :as arq])
  (:import [java.io File]
           (org.openrdf.query QueryInterruptedException)
           (org.apache.jena.query QueryParseException)))

(defn- authenticate-user [user-repo request {:keys [username password] :as auth-data}]
  (if-let [user (user-repo/find-user-by-username user-repo username)]
    (user/try-authenticate user password)))

(defn- basic-auth-backend [user-repo]
  (let [realm "Drafter"
        conf {:realm realm
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
        (when-let [{:keys [email role] :as token} (authproto/-authenticate inner-backend request data)]
          (user/create-authenticated-user email (keyword role)))))))

(defn require-authenticated
  "Requires the incoming request has been authenticated."
  [inner-handler]
  (fn [request]
    (if (auth/authenticated? request)
      (let [email (:email (:identity request))]
        (l4j/with-logging-context {:user email } ;; wrap a logging context over the request so we can trace the user
          (log/info "got user" email)
          (inner-handler request)))
      (auth/throw-unauthorized {:message "Authentication required"}))))

(defn- get-configured-token-auth-backend [env-map]
  (if-let [signing-key (:drafter-jws-signing-key env-map)]
    (jws-auth-backend signing-key)
    (do
      (log/warn "No JWS Token signing key configured - token authentication will not be available")
      (log/warn "To configure JWS Token authentication, set the DRAFTER_JWS_SIGNING_KEY environment variable")
      nil)))

(defn- get-configured-auth-backends [user-repo env-map]
  (let [basic-backend (basic-auth-backend user-repo)
        jws-backend (get-configured-token-auth-backend env-map)]
    (remove nil? [basic-backend jws-backend])))

(defn- wrap-authenticated [auth-backends inner-handler]
  (let [auth-handler (apply wrap-authentication (require-authenticated inner-handler) auth-backends)
        unauthorised-fn (fn [req err]
                          (response/unauthorised-basic-response "Drafter"))]
    (wrap-authorization auth-handler unauthorised-fn)))

(defn make-authenticated-wrapper [user-repo env-map]
  (let [auth-backends (get-configured-auth-backends user-repo env-map)]
    (fn [inner-handler]
      (wrap-authenticated auth-backends inner-handler))))

(defn require-user-role
  "Wraps a handler with one that only permits the request to continue
  if the associated user is in the required role. If not, a 403
  Forbidden response is returned."
  [required-role inner-handler]
  (fn [{:keys [identity] :as request}]
    (if (user/has-role? identity required-role)
      (inner-handler request)
      (response/forbidden-response "User is not authorised to access this resource"))))

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

(defn allowed-methods-handler
  "Wraps a handler with one which checks whether the method of the
  incoming request is allowed with a given predicate. If the request
  method does not pass the predicate a 405 Not Allowed response is
  returned."
  [is-allowed-fn inner-handler]
  (fn [{:keys [request-method] :as request}]
    (if (is-allowed-fn request-method)
      (inner-handler request)
      (response/method-not-allowed-response request-method))))

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

(defn get-sparql-response-content-type [mime-type]
  (case mime-type
    ;; if they ask for html they're probably a browser so serve it as
    ;; text/plain
    "text/html" "text/plain; charset=utf-8"
    ;; force a charset of UTF-8 in this case... NOTE this should
    ;; really consult the Accept-Charset header
    "text/plain" "text/plain; charset=utf-8"
    mime-type))

(defn sparql-negotiation-handler [inner-handler]
  (fn [request]
    (let [query-str (drafter-request/query request)
          accept (drafter-request/accept request)]
      (try
        (let [arq-query (arq/sparql-string->arq-query query-str)
              query-type (arq/get-query-type arq-query)]
          (if-let [[result-format media-type] (conneg/negotiate query-type accept)]
            (inner-handler (assoc request :sparql
                                          {:query query-str
                                           :format result-format
                                           :response-content-type (get-sparql-response-content-type media-type)}))
            (response/not-acceptable-response)))
        (catch QueryParseException ex
          (let [error-message (.getMessage ex)]
            (log/info "Malformed query: " error-message)
            {:status 400 :headers {"Content-Type" "text/plain; charset=utf-8"} :body error-message}))))))