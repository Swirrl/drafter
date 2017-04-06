(ns drafter.backend.configuration
  (:require [clojure.tools.logging :as log]))

(defn get-backend [{:keys [backend-ns] :as config}]
  (let [backend-ns (symbol backend-ns)]
    (log/info "Loading backend from namespace " backend-ns)
    (require backend-ns)
    (let [backend-namespace (ns-map backend-ns)
          fetch-backend (backend-namespace 'get-backend)]
      (fetch-backend config))))
