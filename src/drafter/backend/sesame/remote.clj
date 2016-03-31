(ns drafter.backend.sesame.remote
  (:require [drafter.backend.repository]
            [drafter.backend.sesame.remote.repository :refer [create-repository-for-environment]]))

(defn get-backend [env-map]
  (create-repository-for-environment env-map))
