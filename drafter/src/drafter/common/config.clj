(ns drafter.common.config
  (:require [integrant.core :as ig]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import (java.net URI)))

;; A simple component for storing arbitrary data under a key in an
;; integrant system map.

(defmethod ig/pre-init-spec :drafter.common.config/sparql-query-endpoint [_]
  (s/spec string?))

(defmethod ig/init-key :drafter.common.config/sparql-query-endpoint [_ uri-str]
  (URI. uri-str))

(defmethod ig/pre-init-spec :drafter.common.config/sparql-update-endpoint [_]
  (s/spec string?))

(defmethod ig/init-key :drafter.common.config/sparql-update-endpoint [_ uri-str]
  (URI. uri-str))

(defmethod ig/init-key :drafter.common.config/base-uri [_ uri-str]
  (when-not (str/blank? uri-str)
    (URI. uri-str)))
