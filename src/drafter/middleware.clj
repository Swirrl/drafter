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
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
            [ring.util.request :as request]
            [pantomime.media :refer [media-type-named]])
  (:import [clojure.lang ExceptionInfo]
           [java.io File]))

(defn log-request [handler]
  (fn [req]
    ;; TODO wrap requests with some kind of ID NDC/MDC
    (l4j/with-logging-context {:reqId (-> (Object.) .hashCode)}
      (log/info "REQUEST " (:uri req) (-> req :headers (get "accept")) (:params req))
      (let [start-time (System/currentTimeMillis)
            resp (handler req)
            total-time (- (System/currentTimeMillis) start-time)]
        (log/info "RESPONSE " (:status resp) "took" (str total-time "ms"))
        resp))))

(defn- authenticate-user [user-repo request {:keys [username password] :as auth-data}]
  (if-let [user (user-repo/find-user-by-username user-repo username)]
    (if (user/authenticated? user password)
      user)))

(defn basic-authentication [user-repo realm inner-handler]
  (let [conf {:realm realm
              :authfn #(authenticate-user user-repo %1 %2)
              :unauthorized-handler (fn [req err]
                                      (response/unauthorised-basic-response realm))}
        backend (http-basic-backend conf)]
    (wrap-authorization (wrap-authentication inner-handler backend) backend)))

(defn require-authenticated [inner-handler]
  (fn [request]
    (if (auth/authenticated? request)
      (inner-handler request)
      (auth/throw-unauthorized {:message "Authentication required"}))))

(defn require-basic-authentication
  "Wraps a handler in one which requires the request is authenticated
  through HTTP Basic authentication."
  [user-repo realm inner-handler]
  (basic-authentication user-repo realm (require-authenticated inner-handler)))

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
