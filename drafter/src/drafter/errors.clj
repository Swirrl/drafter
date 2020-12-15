(ns drafter.errors
  "This namespace defines some exception handlers that coerce messages
  across all routes into Ring JSON error responses, according to our
  error object schema (see the lib-swirrl-server project.).

  NOTE:

  More localised errors are probably better handled within specific routes, as
  these handlers dispatched indiscriminantly on anything bubbling up from the call
  stack of any route - hence you may need to be careful about being
  overly-specific in your interpretation of the error."
  (:require [clojure.tools.logging :as log]
            [drafter.async.responses :as r]
            [clojure.spec.alpha :as s]
            [drafter.async.spec :as async]
            [drafter.util :as util]))

(defmulti encode-error
  "Convert an Exception into an appropriate API error response object.
  Dispatches on either the exceptions class or if it's a
  clojure.lang.ExceptionInfo the value of its :error key."
  (fn [err]
    (cond
      (instance? clojure.lang.ExceptionInfo err) (if-let [error-type (-> err ex-data :error)]
                                                   error-type
                                                   (class err))
      (instance? Exception err) (class err))))

(s/fdef encode-error
  :args (s/cat :err util/throwable?)
  :ret ::async/ring-swirrl-error-response)

(defmethod encode-error Throwable [ex]
  ;; The generic catch any possible exception case
  (r/error-response 500 :unknown-error (.getMessage ex)))

(defmethod encode-error clojure.lang.ExceptionInfo [ex]
  ;; Handle ex-info errors that don't define one of our :error keys
  (r/error-response 500 :unknown-error (.getMessage ex)))

(defmethod encode-error :default [ex]
  (if (instance? clojure.lang.ExceptionInfo ex)
    (let [error-type (:error (ex-data ex))]
      (assert error-type "Because the defmulti dispatches clojure.lang.ExceptionInfo's as :keywords we should never have a nil error-type here" )
      (r/error-response 500 error-type (.getMessage ex)))
    (r/error-response 500 :unhandled-error (str "Unknown error: " (.getMessage ex)))))

(defn wrap-encode-errors
  "A ring middleware to handle and render Exceptions and ex-swirrl
  style errors.  You should mix this into your middlewares as high up
  as you can.
  We don't currently do any content negotiation here, so this will
  return all exceptions as JSON."
  [handler]
  (fn [req]
    (try
      (handler req)
      (catch Throwable ex
        (log/error ex "There was an unknown error.  Returning 500")
        (encode-error ex)))))

(defmethod encode-error org.eclipse.rdf4j.query.QueryEvaluationException [ex]
  ;; This exception should also be caught in specific routes concerned with
  ;; SPARQL endpoints, here we use this to unpack our ConnectionPoolTimeoutException.

  ;; TODO: check that the code that uses this - calls encode-error, as it might
  ;; also timeout and warrant a more specific response!!
  (if-let [cause (.getCause ex)]
    (encode-error cause)
    (r/error-response 500
                      :query-evaluation-error
                      (.getMessage ex))))

(defmethod encode-error org.apache.http.conn.ConnectionPoolTimeoutException [ex]
  (r/error-response 503
                    :connection-pool-full
                    (.getMessage ex)))

(defmethod encode-error :reading-aborted [ex]
  (r/error-response 422
                    :rdf-parse-error (.getMessage ex)))
