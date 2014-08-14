(ns drafter.routes.sparql-update
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [ring.util.codec :as codec]
            [compojure.route :refer [not-found]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query]]
            [drafter.rdf.sparql-rewriting :as rew]
            [taoensso.timbre :as timbre]
            [drafter.rdf.sparql-update :as update]
            [grafter.rdf.sesame :as ses]))

(defn do-update [repo restrictions]
  {:status 200})

(defn decode-x-form-urlencoded-parameters
  "Decodes application/x-form-urlencoded parameter strings and returns
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
    (-> request :headers :content-type)))

(defn- read-body [request]
  "Extract the body of the request into a string"
  (-> request :body (slurp :encoding "UTF-8")))

(defmethod parse-update-request "application/sparql-update" [{:keys [body params] :as request}]
  (let [update (read-body request)]
    {:update update
     :graphs (:graphs params)}))

(defmethod parse-update-request "application/x-form-urlencoded" [{:keys [body] :as request}]
  (let [params (-> body read-body decode-x-form-urlencoded-parameters)]
    {:update (get params "update")
     :graphs (get params "graph")}))

(defmethod parse-update-request :default [request]
  (throw (Exception. "Invalid Content-Type")))

(defn process-request [request]
  {:graphs [] :update "input stream"})

(defn execute-update [repo update-query]
  (try
    (ses/with-transaction repo
      (ses/evaluate update-query))
    {:status 200}
    (catch Exception ex
      (timbre/fatal "An exception was thrown when executing a SPARQL update!" ex)
      {:status 500 :body (str "Unknown server error executing update" ex)})))

(defn update-endpoint
  ([mount-point repo]
     (update-endpoint mount-point repo nil))
  ([mount-point repo restrictions]
     (routes
      (POST mount-point request
            (let [{:keys [update graphs]} (parse-update-request request)
                  update-query (if graphs
                                 (update/make-rewritten-update repo update graphs)
                                 (ses/prepare-update repo update))]
              (execute-update repo update-query))))))
