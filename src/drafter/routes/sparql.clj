(ns drafter.routes.sparql
  (:require [compojure.core :refer [make-route]]
            [drafter.rdf
             [sparql-protocol :refer [sparql-end-point sparql-protocol-handler]]]
            [drafter.backend.live :refer [live-endpoint]]
            [integrant.core :as ig]))

(def ^:private v1-prefix :v1)

(defn live-sparql-routes [mount-point executor query-timeout-fn]
  (sparql-end-point mount-point (live-endpoint executor) query-timeout-fn))

(defn- endpoint-query-path [route-name version]
  (let [suffix (str "/sparql/" (name route-name))]
    (if (some? version)
      (str "/" (name version) suffix)
      suffix)))

(defn- get-live-sparql-query-route [backend {:keys [live-query-timeout endpoint-timeout-fn] :as config}]
  (let [mount-point (endpoint-query-path :live v1-prefix)]
    (live-sparql-routes mount-point backend endpoint-timeout-fn)))

(defmethod ig/init-key ::live-sparql-query-route [_ {:keys [repo] :as opts}]
  (get-live-sparql-query-route repo opts))

