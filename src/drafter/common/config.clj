(ns drafter.common.config
  (:require [integrant.core :as ig]))

;; A simple component for storing arbitrary data under a key in an
;; integrant system map.
(defmethod ig/init-key :drafter.common/config [_ opts]
  opts)

