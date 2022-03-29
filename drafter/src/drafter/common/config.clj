(ns drafter.common.config
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
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
  (if-not (str/blank? uri-str)
    (do
      (log/info (str "\nBase URI will be set to `" uri-str "`\n"))
      (URI. uri-str))
    (println (str "\n\u001B[31m"
                  "WARNING: Base URI has not been set."
                  "\u001B[m\n"
                  "You can set Drafter's Base URI via the `DRAFTER_BASE_URI` env var.\n"))))
