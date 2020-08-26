(ns drafter.feature.endpoint.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as st]
            [drafter.feature.endpoint.list :as list]
            [drafter.feature.endpoint.public :as public]
            [drafter.endpoint :as ep]
            [drafter.user :as user]
            [drafter.endpoint.spec]
            [drafter.backend.spec]
            [drafter.user.spec]
            [drafter.backend :as backend]))

(s/fdef list/get-endpoints
        :args (s/cat :repo ::backend/repo :user (s/nilable ::user/User) :include ep/includes :union-with-live? boolean?)
        :ret (s/coll-of ::ep/Endpoint))

(s/fdef public/get-public-endpoint
        :args (s/cat :repo any?)
        :ret (s/nilable ::ep/Endpoint))

(def ^:private fns-with-specs
  [`list/get-endpoints
   `public/get-public-endpoint])

(defn instrument []
  (st/instrument fns-with-specs))

(defn unstrument []
  (st/unstrument fns-with-specs))