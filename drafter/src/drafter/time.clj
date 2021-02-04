(ns drafter.time
  (:require [integrant.core :as ig])
  (:import [java.time OffsetDateTime]))

(defprotocol Clock
  "Represents a source for the current time"
  (now [this]
    "Return the current time according to this clock"))

(defn- make-system-clock
  "Returns a Clock implementation which returns the system UTC time"
  []
  (let [jclock (java.time.Clock/systemUTC)]
    (reify Clock
      (now [_] (OffsetDateTime/now jclock)))))

(def system-clock (make-system-clock))

(defn system-now
  "Returns the current time according to the system clock"
  []
  (now system-clock))

(extend-protocol Clock
  OffsetDateTime
  (now [dt] dt))

(defn parse
  "Parses a string representation of the time into the system representation"
  [s]
  (OffsetDateTime/parse s))

(defmethod ig/init-key ::system-clock [_ _opts]
  system-clock)