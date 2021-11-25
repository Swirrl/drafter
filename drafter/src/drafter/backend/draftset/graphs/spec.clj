(ns drafter.backend.draftset.graphs.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.backend :as backend]
            [drafter.backend.spec]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.time :as time]
            [drafter.time.spec]))

(s/def ::graphs/URIMatcher #(satisfies? graphs/URIMatcher %))

(s/def ::graphs/protected-graphs (s/coll-of ::graphs/URIMatcher :kind set?))

(s/def ::graphs/Manager (s/keys :req-un [::backend/repo
                                         ::graphs/protected-graphs
                                         ::time/clock]))
