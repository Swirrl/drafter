(ns drafter.routes.sparql
  (:require [compojure.core :refer [make-route]]
            [drafter.rdf
             [sparql-protocol :refer [sparql-end-point] :as sp]]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]))

;; TODO: Remove this namespace as all it really adds to
;; sparql-protocol is some route-matching code to /v1/sparql/:name
;; which should really just be moved into config.
;;
;; When we do this we should move the tests also (would suggest into
;; sparql-protocol-test if they're not dupes.)

(def ^:private v1-prefix :v1)

(defn live-sparql-routes [mount-point endpoint query-timeout-fn]
  (sparql-end-point mount-point endpoint query-timeout-fn))

(defn- endpoint-query-path [route-name version]
  (let [suffix (str "/sparql/" (name route-name))]
    (if (some? version)
      (str "/" (name version) suffix)
      suffix)))

(defn- get-live-sparql-query-route [backend {:keys [timeout-fn] :as config}]
  (let [mount-point (endpoint-query-path :live v1-prefix)]
    (live-sparql-routes mount-point backend (or timeout-fn sp/default-query-timeout-fn))))

(defmethod ig/pre-init-spec ::live-sparql-query-route [_]
  (s/keys :opt-un [::sp/timeout-fn]))

(defmethod ig/init-key ::live-sparql-query-route [_ {:keys [repo] :as opts}]
  (get-live-sparql-query-route repo opts))

