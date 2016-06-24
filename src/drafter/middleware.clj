(ns drafter.middleware
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [environ.core :refer [env]]
            [selmer.parser :as parser]
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
            [pantomime.media :refer [media-type-named]])
  (:import [clojure.lang ExceptionInfo]
           [java.io File]
           [java.util UUID]))

(defn log-request [handler]
  (fn [req]
    ;; TODO wrap requests with some kind of ID NDC/MDC
    (let [start-time (System/currentTimeMillis)]
      (l4j/with-logging-context {:reqId (str "req-" (-> (UUID/randomUUID) str (.substring 0 8)))
                                 :start-time start-time}
        (log/info "REQUEST" (:uri req) (-> req :headers (get "accept")) (:params req))
        (let [resp (handler req)
              headers-time (- (System/currentTimeMillis) start-time)]
          (if (instance? java.io.InputStream (:body resp))
            (log/info "RESPONSE " (:status resp) "headers sent after" (str headers-time "ms") "streaming body...")
            (log/info "RESPONSE " (:status resp) "finished.  It took" (str headers-time "ms") "to execute"))
          resp)))))

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

(defn basic-authentication
  "Requires the incoming request is authenticated using basic
  authentication."
  [user-repo inner-handler]
  (let [backend (basic-auth-backend user-repo)]
    (wrap-authorization (wrap-authentication inner-handler backend) backend)))

(defn require-authenticated
  "Requires the incoming request has been authenticated."
  [inner-handler]
  (fn [request]
    (if (auth/authenticated? request)
      (inner-handler request)
      (auth/throw-unauthorized {:message "Authentication required"}))))

(defn require-basic-authentication
  "Wraps a handler in one which requires the request is authenticated
  through HTTP Basic authentication."
  [user-repo inner-handler]
  (basic-authentication user-repo (require-authenticated inner-handler)))

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
