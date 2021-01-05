(ns drafter.backend.spec
  (:require [clojure.spec.alpha :as s]
            [grafter-2.rdf4j.repository :as repo]
            [clojure.spec.gen.alpha :as gen]
            [drafter.backend :as backend]
            [drafter.backend.live :as live]
            [drafter.backend.draftset :as draftset]
            [integrant.core :as ig]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs for backend config etc.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(s/def ::backend/repo (s/with-gen #(satisfies? repo/ToConnection %)
                          (fn [] (gen/return (repo/sail-repo)))))

(defmethod ig/pre-init-spec :drafter/backend [_]
  (s/keys :req-un [::backend/repo]))

(s/def ::backend/DraftsetGraphMapping (s/map-of uri? uri?))

(defmethod ig/pre-init-spec ::live/endpoint [_]
  (s/keys :req-un [::backend/repo]))
