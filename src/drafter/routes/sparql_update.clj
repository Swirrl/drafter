(ns drafter.routes.sparql-update
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [POST]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.write-scheduler :as scheduler]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-rewriting :as rew]
            [grafter.rdf.repository :refer [->connection evaluate
                                            make-restricted-dataset
                                            prepare-update
                                            with-transaction]]
            [pantomime.media :as mt]
            [ring.util.codec :as codec])
  (:import (org.openrdf.query Dataset)
           (org.openrdf.repository RepositoryConnection)))

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

(defn- collection-or-nil? [x]
  (or (coll? x)
      (nil? x)))

(defn- collection-nil-or-fn? [x]
  (or (collection-or-nil? x)
      (ifn? x)))

(defn resolve-restrictions
  "If restrictions is a sequence or nil return it, otherwise assume
  it's a 0 arg function and call it returning its parameters."
  [restrictions-or-f]
  {:pre [(collection-nil-or-fn? restrictions-or-f)]
   :post [(or (instance? Dataset %) (nil?  %))]}
  (let [restrictions (if (collection-or-nil? restrictions-or-f)
                       restrictions-or-f
                       (restrictions-or-f))]
    (when (seq restrictions)
      (make-restricted-dataset :default-graph restrictions
                               :union-graph restrictions))))

(defn- lift [g]
  (if (instance? String g)
    [g]
    g))

(defn prepare-restricted-update [repo update-str graphs]
  (let [restricted-ds (when (seq graphs)
                        (resolve-restrictions graphs))]
    (prepare-update repo update-str restricted-ds)))

(defn create-update-job [repo request prepare-fn]
  (jobs/make-job :sync-write [job]
                 (with-open [conn (->connection repo)]
                   (let [parsed-query (parse-update-request request)
                         prepared-update (prepare-fn parsed-query conn)]
                     (log/debug "Executing update-query " prepared-update)
                     (with-transaction conn
                       (evaluate prepared-update))
                     (scheduler/complete-job! job {:status 200 :body "OK"})))))

;exec-update :: Repository -> Request -> (ParsedStatement -> Connection -> PreparedStatement) -> Response
(defn exec-update [repo request prepare-fn]
  (let [job (create-update-job repo request prepare-fn)]
    (scheduler/submit-sync-job! job)))

(defn prepare-update-statement [{update :update} conn restrictions]
  (let [rs (if (fn? restrictions) (restrictions) restrictions)]
    (prepare-restricted-update conn update rs)))

(defn update-endpoint
  "Create a standard update-endpoint with no query-rewriting and
  optional restrictions on the allowed graphs; restrictions can either
  be a collection of string graph-uri's or a function that returns
  such a collection."
  ([mount-point repo]
     (update-endpoint mount-point repo #{}))
  ([mount-point repo restrictions]
     (POST mount-point request
           (exec-update repo request #(prepare-update-statement %1 %2 restrictions)))))

(defn prepare-draft-update [{:keys [update graphs]} conn]
  (let [graphs (lift graphs)
        preped-update (prepare-restricted-update conn update graphs)]
    (rew/rewrite-update-request preped-update (mgmt/graph-map conn graphs))
    preped-update))

(defn draft-update-endpoint
  "Create an update endpoint with draft query rewriting.  Restrictions
  are applied on the basis of the &graphs query parameter."
  ([mount-point repo]
   (POST mount-point request
         (exec-update repo request prepare-draft-update))))

(defn draft-update-endpoint-route [mount-point repo]
  (draft-update-endpoint mount-point repo))

(defn live-update-endpoint-route [mount-point repo]
  (update-endpoint mount-point repo (partial mgmt/live-graphs repo)))

(defn state-update-endpoint-route [mount-point repo]
  (update-endpoint mount-point repo #{mgmt/drafter-state-graph}))

(defn raw-update-endpoint-route [mount-point repo]
  (update-endpoint mount-point repo))
