(ns drafter.backend.spec
  (:require [clojure.spec.alpha :as s]
            [grafter-2.rdf4j.repository :as repo]
            [clojure.spec.gen.alpha :as gen]
            [drafter.backend :as backend]
            [drafter.backend.live :as live]
            [drafter.backend.draftset :as draftset]
            [integrant.core :as ig])
  (:import [grafter_2.rdf4j.repository ToConnection]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs for backend config etc.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(s/def ::repo (s/with-gen #(satisfies? ToConnection %)
                          (fn [] (gen/return (repo/sail-repo)))))

(defmethod ig/pre-init-spec :drafter/backend [_]
  (s/keys :req-un [::backend/repo]))

(defmethod ig/pre-init-spec ::live/endpoint [_]
  (s/keys :req-un [::backend/repo]))

(defmethod ig/pre-init-spec ::draftset/endpoint [_]
  (s/keys :req-un [::backend/repo]))

