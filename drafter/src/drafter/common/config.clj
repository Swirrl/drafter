(ns drafter.common.config
  (:require [integrant.core :as ig]
            [clojure.spec.alpha :as s]))

;; A simple component for storing arbitrary data under a key in an
;; integrant system map.

(defmethod ig/pre-init-spec :drafter.common.config/sparql-query-endpoint [_]
  (s/spec string?))

(defmethod ig/init-key :drafter.common.config/sparql-query-endpoint [_ uri-str]
  (java.net.URI. uri-str))

(defmethod ig/pre-init-spec :drafter.common.config/sparql-update-endpoint [_]
  (s/spec string?))

(defmethod ig/init-key :drafter.common.config/sparql-update-endpoint [_ uri-str]
  (java.net.URI. uri-str))


;; TODO: we should probably remove this component now we've moved its
;; properties :sparql-query-endpoint and :sparql-update-endpoint to
;; the top-level system.
(defmethod ig/init-key :drafter.common/config [_ opts]
  opts)
