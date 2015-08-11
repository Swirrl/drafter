(ns drafter.routes.sparql-update
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [POST]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [complete-job!]]
            [swirrl-server.responses :as response]
            [drafter.responses :refer [default-job-result-handler submit-sync-job!]]
            [drafter.rdf.draft-management :as mgmt]
            [ring.util.codec :as codec]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point restricted-dataset]]
            [drafter.operations :as ops]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository :refer [with-transaction make-restricted-dataset
                                            prepare-update evaluate ToConnection ->connection]]
            [pantomime.media :as mt]
            [drafter.common.sparql-routes :refer [supplied-drafts]])
  (:import [java.util.concurrent FutureTask CancellationException]
           [org.openrdf.query Dataset]
           [org.openrdf.repository Repository RepositoryConnection]))

(defn do-update [repo restrictions]
  {:status 200})

(defn decode-x-form-urlencoded-parameters
  "Decodes application/x-www-form-urlencoded parameter strings and returns
  a hashmap mapping.  If a value occurs multiple times the repeated
  values are stored under that key in a vector.

  This is necessary to support the presence of multiple graph
  parameters."
  [s]
  (letfn [(multi-value-merge [existing new]
            (if (vector? existing) (conj existing new) [existing new]))]

    (->> (clojure.string/split s #"&")
         (map codec/url-decode)
         (map (fn [s] (clojure.string/split s #"=")))
         (map (partial apply hash-map))
         (apply merge-with multi-value-merge))))

(defmulti parse-update-request
  "Convert a request into an String representing the SPARQL update
  query depending on the content-type."
  (fn [request]
    (let [mime (mt/base-type (:content-type request))]
      (str (.getType mime) \/ (.getSubtype mime)))))

(defn- read-body [body]
  "Extract the body of the request into a string"
  (-> body (slurp :encoding "UTF-8")))

(defmethod parse-update-request "application/sparql-update" [{:keys [body params] :as request}]
  (let [update (read-body body)]
    {:update update
     :graphs (get params "graph")}))

(defmethod parse-update-request "application/x-www-form-urlencoded" [request]
  (let [params (-> request :form-params)]
    {:update (get params "update")
     :graphs (get params "graph")}))

(defmethod parse-update-request :default [request]
  (throw (Exception. (str "Invalid Content-Type " (:content-type request)))))

(defn prepare-restricted-update [repo update-str graphs]
  (let [restricted-ds (when (seq graphs)
                        (restricted-dataset graphs))]
    (prepare-update repo update-str restricted-ds)))

(defn create-update-job [repo request prepare-fn timeouts]
  (jobs/make-job :sync-write [job]
                 (with-open [conn (->connection repo)]
                   (let [timeouts (or timeouts ops/default-timeouts)
                         parsed-query (parse-update-request request)
                         prepared-update (prepare-fn parsed-query conn)
                         update-future (FutureTask. (fn []
                                                      (with-transaction conn
                                                        (evaluate prepared-update))))]
                     (try
                       (log/debug "Executing update-query " prepared-update)
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
                         (throw ex)))))))

(def ^:private sparql-update-applied-response {:status 200 :body "OK"})

;exec-update :: Repository -> Request -> (ParsedStatement -> Connection -> PreparedStatement) -> Response
(defn exec-update [repo request prepare-fn timeouts]
  (let [job (create-update-job repo request prepare-fn timeouts)]
    (submit-sync-job! job (fn [result]
                            (if (jobs/failed-job-result? result)
                              (response/api-response 500 result)
                              sparql-update-applied-response)))))

(defn prepare-update-statement [{update :update} conn restrictions]
  (let [rs (if (fn? restrictions) (restrictions) restrictions)]
    (prepare-restricted-update conn update rs)))

(defn update-endpoint
  "Create a standard update-endpoint and optional restrictions on the
  allowed graphs; restrictions can either be a collection of string
  graph-uri's or a function that returns such a collection."

  ([mount-point repo]
     (update-endpoint mount-point repo #{}))

  ([mount-point repo restrictions]
   (update-endpoint mount-point repo restrictions nil))

  ([mount-point repo restrictions timeouts]
     (POST mount-point request
           (exec-update repo request #(prepare-update-statement %1 %2 restrictions) timeouts))))

(defn live-update-endpoint-route [mount-point repo timeouts]
  (update-endpoint mount-point repo (partial mgmt/live-graphs repo) timeouts))

(defn state-update-endpoint-route [mount-point repo timeouts]
  (update-endpoint mount-point repo #{mgmt/drafter-state-graph} timeouts))

(defn raw-update-endpoint-route [mount-point repo timeouts]
  (update-endpoint mount-point repo nil timeouts))
