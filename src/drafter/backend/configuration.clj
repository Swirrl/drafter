(ns drafter.backend.configuration
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]))

(defn get-backend [env-map]
  (let [backend-ns (symbol (get env-map :drafter-backend "drafter.backend.sesame.remote"))]
    (log/info "Loading backend from namespace " backend-ns)
    (require backend-ns)
    (let [backend-namespace (ns-map backend-ns)
          fetch-backend (backend-namespace 'get-backend)]
      (fetch-backend env-map))))
