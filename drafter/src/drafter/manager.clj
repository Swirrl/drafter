(ns drafter.manager
  (:require [integrant.core :as ig]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.write-scheduler :as writes]
            [drafter.time :as time]))

(defn create-manager
  ([repo] (create-manager repo {}))
  ([repo {:keys [clock graph-manager global-writes-lock] :as opts}]
   (let [clock (or clock time/system-clock)
         graph-manager (or graph-manager (graphs/create-manager repo #{} clock))
         global-writes-lock (or global-writes-lock (writes/create-writes-lock))]
     {:backend repo :global-writes-lock global-writes-lock :graph-manager graph-manager :clock clock})))

(defmethod ig/init-key :drafter/manager [_ {:keys [drafter/backend
                                                   drafter/global-writes-lock
                                                   drafter.time/clock
                                                   ::graphs/manager] :as opts}]
  (create-manager backend {:clock clock :graph-manager manager :global-writes-lock global-writes-lock}))

