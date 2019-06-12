(ns drafter-client.client.spec
  (:require [clojure.spec.alpha :as s]
            [drafter-client.client.auth :as auth]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.repo :as repo]
            [martian.core :as m]))

(s/def ::m/api-root string?)
(s/def ::m/handlers any?)
(s/def ::m/interceptors any?)
(s/def ::m/martian (s/keys :req-un [::m/api-root ::m/handlers ::m/interceptors]))

(s/def ::jws-key string?)
(s/def ::api (s/merge ::m/martian (s/keys :req-un [::jws-key])))
(s/def ::client (partial instance? drafter_client.client.impl.DrafterClient))

(s/def ::context (s/or :live draftset/live? :draft draftset/draft?))

(s/def ::params (s/map-of keyword? any?))

(s/def ::role string?)
(s/def ::email string?)
(s/def ::user (s/keys :req-un [::role ::email]))
(s/def ::token string?)


(s/fdef repo/make-repo
  :args (s/cat :client ::client :context ::context :token ::token :params ::params)
  :ret (partial instance? org.eclipse.rdf4j.repository.Repository))
