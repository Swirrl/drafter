(ns drafter.endpoint.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.endpoint :as ep])
  (:import [java.time OffsetDateTime]))

(defn date-time? [v]
  (instance? OffsetDateTime v))

(s/def ::ep/id any?)
(s/def ::ep/type #{"Draftset" "Endpoint"})
(s/def ::ep/created-at date-time?)
(s/def ::ep/updated-at date-time?)

(s/def ::ep/Endpoint (s/keys :req-un [::ep/id ::ep/type ::ep/created-at ::ep/updated-at]))

