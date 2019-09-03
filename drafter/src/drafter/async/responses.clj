(ns drafter.async.responses
  (:require [compojure.core :refer [GET routes]]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found response]]
            [clojure.spec.alpha :as s]
            [drafter.async.spec :as async]
            [clj-time.core :as time]
            [clojure.test.check.generators :as gen])
  (:import [java.util UUID]))

(defonce restart-id (UUID/randomUUID))
(defonce restart-time (time/now))

(defn try-parse-uuid
  "Tries to parse a String into a UUID and returns nil if the
  parse failed."
  [s]
  (when s
    (try
      (UUID/fromString s)
      (catch IllegalArgumentException ex
        nil))))

(defn json-response
  "Returns a ring map representing a HTTP response with the given code
  and map as a JSON body."
  [code map]
  {:status code
   :headers {"Content-Type" "application/json"}
   :body map})

(def default-response-map {:type :ok})

(def default-error-map {:type :error
                        :error :unknown-error
                        :message "An unknown error occured"})

(defn api-response
  "Returns a JSON response containing a SwirrlObject document in the
  body. If a map argument is provided it will be attached to the JSON
  document under the :details key"
  ([code] (json-response code default-response-map))
  ([code map]
   (json-response code (assoc default-response-map :details map))))

(s/fdef submitted-job-response
  :args (s/or :ary-1 (s/cat :job ::async/job)
              :ary-2 (s/cat :prefix-path string? :job ::async/job))
  :ret :submitted-job/response)

(defn submitted-job-response
  ([job] (submitted-job-response "" job))
  ([prefix-path {:keys [id] :as job}]
   (json-response 202 {:type :ok
                       :finished-job (format "/v1/status/finished-jobs/%s" id)
                       :restart-id restart-id})))

(s/fdef job-not-finished-response
  :args (s/cat :restart-id ::async/restart-id)
  :ret :job-not-finished/response)

(defn job-not-finished-response [restart-id]
  (json-response 404
                 {:type :not-found
                  :message "The specified job-id was not found"
                  :restart-id restart-id}))

(def ok-response
  "Returns a 200 ok response, with a JSON message body containing
  {:type :ok}"
  (json-response 200 {:type :ok}))

(s/def ::throwable
  (s/with-gen (partial instance? java.lang.Throwable)
    (fn [] (gen/fmap #(Exception. %) (s/gen string?)))))

(s/def ::keyword-or-throwable
  (s/or :keyword keyword?
        :throwable ::throwable))

(s/fdef error-response
  :args (s/or :ary-0 empty?
              :ary-1 (s/cat :code int?)
              :ary-2 (s/cat :code int?
                            :error-type ::keyword-or-throwable)
              :ary-3 (s/cat :code int?
                            :error-type ::keyword-or-throwable
                            :msg (s/nilable string?))
              :ary-4 (s/cat :code int?
                            :error-type ::keyword-or-throwable
                            :msg (s/nilable string?)
                            :data map?))
  :ret ::async/ring-swirrl-error-response)

(defn error-response
  "Build a ring response containing a JSON error object.

   The intention is that you can use this with encode-error to override the JSON
   rendering of specific exceptions with specific ring responses.

   For example,

   (error-response 412 :my-error) ;; returns ex-info's of :error type :my-error
   as 412's.

   It can also coerce exceptions:

   (error-response 422 (RuntimeException. \"a message\"))

   And you can override messages held within the exception with something more
   bespoke:

   (error-response 422 (RuntimeException. \"ignore this message\") \"Use this message\")"

  ([]
   (error-response 500))

  ([code]
   (error-response code :unknown-error))

  ([code error-type]
   (error-response code error-type nil))

  ([code error-type msg]
   (error-response code error-type msg {}))

  ([code error-type msg data]

   (let [error-obj (cond
                     (keyword? error-type) {:error error-type :message msg}
                     (instance? java.lang.Throwable error-type) (let [error-keyword (-> error-type ex-data :error)]
                                                                  {:error error-keyword
                                                                   :message (.getMessage error-type)}))
         retain-by-identity #(if %1 (if %2 %2 %1) %2)

         ;; define a priority order for overriding :error and :message
         ;; we prefer data the least (to prevent collisions) and args the most
         ;; the retain-by-identity function above keeps overrides the preferred
         ;; value unless the value is nil.
         args {:error (when (keyword? error-type) error-type) :message msg}
         priority [data default-error-map error-obj args]]

     (json-response code
                   (apply merge-with retain-by-identity
                          priority)))))

(s/fdef bad-request-response
  :args (s/cat :s string?)
  :ret ::async/ring-swirrl-error-response)

(defn bad-request-response
  "Returns a 'bad request' response from the given error message."
  [s]
  (error-response 422 :invalid-parameters s))

(defn unauthorised-basic-response [realm]
  (let [params (str "Basic realm=\"" realm "\"")]
    {:status 401 :body "" :headers {"WWW-Authenticate" params}}))

(defmacro when-params
  "Simple macro that takes a set of paramaters and tests that they're
  all truthy.  If any are falsey it returns an appropriate ring
  response with an error message.  The error message assumes that the
  symbol name is the same as the HTTP parameter name."
  [params & form]
  `(if (every? identity ~params)
     ~@form
     (let [missing-params# (string/join (interpose ", " (quote ~params)))
           message# (str "You must supply the parameters " missing-params#)]
       (bad-request-response message#))))
