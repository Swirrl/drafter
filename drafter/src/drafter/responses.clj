(ns drafter.responses
  (:require [clojure.string :refer [upper-case]]
            [drafter.async.responses :as r]
            [drafter.errors :refer [encode-error]]))

(defn not-acceptable-response
  ([] (not-acceptable-response ""))
  ([body] {:status 406 :headers {} :body body}))

(defn unprocessable-entity-response [body]
  {:status 422 :headers {} :body body})

(defn unsupported-media-type-response [body]
  {:status 415 :headers {} :body body})

(defn method-not-allowed-response [method]
  {:status 405
   :headers {}
   :body (str "Method " (upper-case (name method)) " not supported by this resource")})

(defmethod encode-error :writes-temporarily-disabled [ex]
  (r/error-response 503 ex))

(defmethod encode-error :forbidden [ex]
  (r/error-response 403 ex))

(defmethod encode-error :payload-too-large [ex]
  (r/error-response 413 ex))

(defmethod encode-error :bad-request [ex]
  (r/error-response 400 ex))

(defmethod encode-error :unprocessable-request [ex]
  (r/error-response 413 ex))

(defmethod encode-error :method-not-allowed [ex]
  (method-not-allowed-response (:method (ex-data ex))))

(defn unauthorised-basic-response [realm]
  (let [params (str "Basic realm=\"" realm "\"")]
    {:status 401 :body "" :headers {"WWW-Authenticate" params}}))

(defn unauthorized-response [body]
  {:status 401
   :headers {"Content-Type" "text/plain"}
   :body body})

(defn forbidden-response [body]
  {:status 403 :body body :headers {}})

(defn conflict-detected-response [body]
  {:status 409 :body body :headers {}})
