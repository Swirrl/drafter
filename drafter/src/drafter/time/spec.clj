(ns drafter.time.spec
  (:require [drafter.time :as time]
            [clojure.spec.alpha :as s])
  (:import [java.time OffsetDateTime]))

(s/def ::time/time #(instance? OffsetDateTime %))
(s/def ::time/clock #(satisfies? time/Clock %))

(s/fdef time/now
  :args (s/cat :clock ::time/clock)
  :ret ::time/time)

(s/fdef time/parse
  :args (s/cat :s string?)
  :ret ::time/time)
