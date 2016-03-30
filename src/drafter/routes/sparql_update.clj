(ns drafter.routes.sparql-update
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [POST]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [complete-job!]]
            [swirrl-server.responses :as response]
            [swirrl-server.errors :refer [ex-swirrl]]
            [drafter.responses :refer [submit-sync-job!]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.protocols :refer [execute-update]]
            [drafter.rdf.endpoints :refer [live-endpoint state-endpoint]]
            [drafter.operations :as ops]
            [pantomime.media :as mt])
  (:import [java.util.concurrent FutureTask CancellationException]))

(defmulti parse-update-request
  "Convert a request into an String representing the SPARQL update
  query depending on the content-type."
  (fn [request]
    (let [mime (mt/base-type (:content-type request))]
      (str (.getType mime) \/ (.getSubtype mime)))))

(defn- read-body [body]
  "Extract the body of the request into a string"
  (slurp body :encoding "UTF-8"))

(defmethod parse-update-request "application/sparql-update" [{:keys [body params] :as request}]
  (let [update (read-body body)]
    {:update update
     :graphs (get params "graph")}))

(defmethod parse-update-request "application/x-www-form-urlencoded" [request]
  (let [params (:form-params request)]
    {:update (get params "update")
     :graphs (get params "graph")}))

(defmethod parse-update-request :default [request]
  (throw (ex-swirrl :invalid-content-type (str "Invalid Content-Type: " (:content-type request)))))

(defn create-update-job [executor request timeouts]
  (jobs/make-job :sync-write [job]
                 (let [timeouts (or timeouts ops/default-timeouts)
                       parsed-query (parse-update-request request)
                       query-string (:update parsed-query)
                       update-future (FutureTask. #(execute-update executor query-string))]
                   (try
                     (log/debug "Executing update-query " parsed-query)
                     ;; The 'reaper' framework monitors instances of the
                     ;; Future interface and cancels them if they timeout
                     ;; create a Future for the update, register it for
                     ;; cancellation on timeout and then run it on this
                     ;; thread.
                     (ops/register-for-cancellation-on-timeout update-future timeouts)
                     (.run update-future)
                     (.get update-future)
                     (complete-job! job {:type :ok})
                     (catch CancellationException cex
                       ;; update future was run on the current
                       ;; thread so it was interrupted when the
                       ;; future was cancelled clear the interrupted
                       ;; flag on this thread
                       (Thread/interrupted)
                       (log/fatal cex "Update operation cancelled due to timeout")
                       (throw cex))
                     (catch Exception ex
                       (log/fatal ex "An exception was thrown when executing a SPARQL update!")
                       (throw ex))))))

(def ^:private sparql-update-applied-response {:status 200 :body "OK"})

;exec-update :: SparqlUpdateExecutor -> Request -> Response
(defn exec-update [executor request timeouts]
  (let [job (create-update-job executor request timeouts)]
    (submit-sync-job! job (fn [result]
                            (if (jobs/failed-job-result? result)
                              ;; NOTE: We could repackage the resulting error
                              ;; into a ex-info exception and throw it to
                              ;; restore the status code and other details from
                              ;; the response that have been lost, if we want more precise
                              (response/error-response 500 result)
                              sparql-update-applied-response)))))

(defn update-endpoint
  "Create an endpoint for executing SPARQL updates."
  ([mount-point executor]
   (update-endpoint mount-point executor nil))

  ([mount-point executor timeouts]
     (POST mount-point request
           (exec-update executor request timeouts))))

(defn live-update-endpoint-route [mount-point backend timeouts]
  (update-endpoint mount-point (live-endpoint backend) timeouts))

(defn state-update-endpoint-route [mount-point backend timeouts]
  (update-endpoint mount-point (state-endpoint backend) timeouts))

(defn raw-update-endpoint-route [mount-point backend timeouts]
  (update-endpoint mount-point backend timeouts))
