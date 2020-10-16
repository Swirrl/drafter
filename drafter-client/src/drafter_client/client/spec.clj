(ns ^{:deprecated "Use drafter-client.client-spec"} drafter-client.client.spec
  (:require [clojure.spec.alpha :as s]
            [martian.core :as m]))

;; TODO move these specs into client_spec.clj and ensure they're used,
;; or delete them.
(s/def ::m/api-root string?)
(s/def ::m/handlers any?)
(s/def ::m/interceptors any?)
(s/def ::m/martian (s/keys :req-un [::m/api-root ::m/handlers ::m/interceptors]))

(s/def ::jws-key string?)
(s/def ::api (s/merge ::m/martian (s/keys :req-un [::jws-key])))
