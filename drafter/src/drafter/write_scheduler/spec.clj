(ns drafter.write-scheduler.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.write-scheduler :as write-scheduler]
            [integrant.core :as ig])
  (:import [java.util.concurrent TimeUnit]
           [java.util.concurrent.locks ReentrantLock]))

(s/def :named/unit (set (keys write-scheduler/timeunit)))
(s/def :java/unit #(instance? TimeUnit %))

(s/def ::write-scheduler/lock #(instance? ReentrantLock %))
(s/def ::write-scheduler/time pos-int?)
(s/def ::write-scheduler/unit :java/unit)
(s/def ::write-scheduler/fairness boolean?)

(s/def ::write-scheduler/WritesLock (s/keys :req-un [::write-scheduler/lock
                                                     ::write-scheduler/time
                                                     :java/unit]))

(defmethod ig/pre-init-spec :drafter/write-scheduler [_]
  (s/keys :req [:drafter/global-writes-lock]))

(s/fdef write-scheduler/create-writes-lock
  :args (s/alt :default (s/cat)
               :opts (s/keys :opt-un [::write-scheduler/fairness
                                      ::write-scheduler/time
                                      :named/unit]))
  :ret ::write-scheduler/WritesLock)
