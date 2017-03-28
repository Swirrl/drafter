(ns drafter.routes.sparql-update
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [POST make-route]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [job-succeeded!]]
            [swirrl-server.responses :as response]
            [swirrl-server.errors :refer [ex-swirrl]]
            [drafter.responses :refer [submit-sync-job!]]
            [drafter.backend.protocols :refer [prepare-update]]
            [drafter.rdf.endpoints :refer [live-endpoint]]
            [drafter.timeouts :as timeouts]
            [drafter.middleware :refer [require-user-role]]
            [pantomime.media :as mt]
            [drafter.requests :as request]
            [drafter.user :as user]))

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

(defn create-update-job [executor request endpoint-timeout]
  (jobs/make-job :sync-write [job]
                 (let [user (request/get-user request)
                       query-timeout-seconds (timeouts/calculate-query-timeout nil (user/max-query-timeout user) endpoint-timeout)
                       parsed-query (parse-update-request request)
                       query-string (:update parsed-query)
                       pquery (prepare-update executor query-string)]
                   (.setMaxExecutionTime pquery query-timeout-seconds)
                   (.execute pquery)
                   (job-succeeded! job))))

(def ^:private sparql-update-applied-response {:status 200 :body "OK"})

;exec-update :: SparqlUpdateExecutor -> Request -> Response
(defn exec-update [executor request timeouts]
  (let [job (create-update-job executor request timeouts)]
    (submit-sync-job! job (fn [result]
                            (if (jobs/failed-job-result? result)
                              ;; NOTE: We could repackage the resulting error
                              ;; into a ex-info exception and throw it to
                              ;; restore the status code and other details from
                              ;; the response that have been lost, if we want
                              ;; to be more precise
                              (response/error-response 500 result)
                              sparql-update-applied-response)))))

(defn- update-request-handler [executor timeouts]
  (fn [request]
    (exec-update executor request timeouts)))

(defn update-endpoint
  "Create an endpoint for executing SPARQL updates."
  ([mount-point executor]
   (update-endpoint mount-point executor nil))

  ([mount-point executor timeouts]
   (make-route :post mount-point (update-request-handler executor timeouts))))

(defn live-update-endpoint-route [mount-point backend timeouts]
  (update-endpoint mount-point (live-endpoint backend) timeouts))

(defn raw-update-endpoint-route [mount-point backend timeouts authenticated-fn]
  (->> (update-request-handler backend timeouts)
       (require-user-role :system)
       (authenticated-fn)
       (make-route :post mount-point)))
