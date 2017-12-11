(ns drafter.backend
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.spec :as bs]
            [drafter.backend.draftset :as draftsets]
            [drafter.backend.live :as live]
            [integrant.core :as ig])
  (:import org.eclipse.rdf4j.repository.Repository))

(defmulti endpoint-repo
  "Given a drafter backend and an endpoint id, return a repository on
  the endpoint."
  (fn [drafter endpoint-id opts]
    (if (keyword? endpoint-id)
      endpoint-id
      (type endpoint-id))))

;; named endpoint's
(defmethod endpoint-repo ::live [drafter endpoint-id opts]
  (live/live-endpoint-with-stasher drafter))


;; draftsets
(defmethod endpoint-repo String [drafter draftset-ref {:keys [union-with-live?]}]
  (draftsets/build-draftset-endpoint drafter draftset-ref union-with-live?))


(defmethod ig/init-key :drafter/backend [_ opts]
  opts)


