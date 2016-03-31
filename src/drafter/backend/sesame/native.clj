(ns drafter.backend.sesame.native
  (:require [drafter.backend.sesame.native.repository :refer [get-repository]]
            [drafter.backend.repository]))

(defn get-backend [env-map]
  (get-repository env-map))
