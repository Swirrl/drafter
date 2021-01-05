(ns drafter.manager.spec
  (:require [drafter.manager :as manager]
            [drafter.time :as time]
            [drafter.time.spec]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.backend.draftset.graphs.spec]
            [drafter.backend :as backend]
            [drafter.backend.spec]
            [drafter.write-scheduler :as write-scheduler]
            [drafter.write-scheduler.spec]
            [clojure.spec.alpha :as s]))

(s/def ::manager/backend ::backend/repo)
(s/def ::manager/graph-manager ::graphs/Manager)
(s/def ::manager/global-writes-lock ::write-scheduler/WritesLock)

(s/def :drafter/manager (s/keys :req-un [::manager/backend
                                         ::manager/graph-manager
                                         ::manager/global-writes-lock
                                         ::time/clock]))

(s/fdef manager/create-manager
  :args (s/alt :default (s/cat :repo ::backend/repo)
               :opts (s/cat :repo ::backend/repo :opts (s/keys :opt-un [::manager/graph-manager
                                                                        ::manager/global-writes-lock
                                                                        ::time/clock])))
  :ret :drafter/manager)
