(ns drafter.routes.sparql
  (:require [compojure.core :refer [make-route]]
            [drafter.rdf
             [sparql-protocol :refer [sparql-end-point] :as sp]]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]
            [drafter.timeouts :as timeouts]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [ring.middleware.cors :as cors]))

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

(def cors-allowed-headers
  #{"Accept"
    "Accept-Encoding"
    "Authorization"
    "Cache-Control"
    "Content-Type"
    "DNT"
    "If-Modified-Since"
    "Keep-Alive"
    "User-Agent"
    "X-CustomHeader"
    "X-Requested-With"})

(defmethod ig/init-key ::live-sparql-query-route [_ {:keys [repo] :as opts}]
  (-> (get-live-sparql-query-route repo opts)
      (wrap-encode-errors)
      (cors/wrap-cors :access-control-allow-headers cors-allowed-headers
                      :access-control-allow-methods [:get :options :post]
                      :access-control-allow-origin #".*")))
