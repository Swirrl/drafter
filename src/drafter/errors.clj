(ns drafter.errors
 "This namespace defines some exception handlers that coerce messages
  across all routes into Ring JSON error responses, according to our
  error object schema (see the lib-swirrl-server project.).

NOTE:

More localised errors are probably better handled within specific routes, as
these handlers dispatched indiscriminantly on anything bubbling up from the call
stack of any route - hence you may need to be careful about being
overly-specific in your interpretation of the error."

  (:require [swirrl-server.errors :refer [encode-error]]
            [swirrl-server.responses :as r]
            [schema.core :as s]))


(defmethod encode-error org.openrdf.query.QueryEvaluationException [ex]
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
