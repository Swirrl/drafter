(ns drafter.draftset.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.draftset :as ds]
            [drafter.endpoint :as ep]
            [drafter.spec]
            [drafter.endpoint.spec]
            [clojure.spec.test.alpha :as st]))

(s/def ::ds/draftset-ref #(satisfies? ds/DraftsetRef %))

(s/def ::ds/status #{:created :updated :deleted})
(s/def ::ds/changes (s/map-of :drafter/URI (s/keys :req-un [::status])))
(s/def ::ds/created-by :drafter/EmailAddress)
(s/def ::ds/display-name string?)
(s/def ::ds/description string?)
(s/def ::ds/submitted-by :drafter/EmailAddress)
(s/def ::ds/current-owner :drafter/EmailAddress)
(s/def ::ds/claim-user string?)
(s/def ::ds/claim-role keyword?)

(s/def ::ds/HasDescription (s/keys :req-un [::ds/description]))
(s/def ::ds/HasDisplayName (s/keys :req-un [::ds/display-name]))
(s/def ::ds/DraftsetCommon (s/merge ::ep/Endpoint
                                    (s/keys :req-un [::ds/changes ::ds/created-by]
                                            :opt-un [::ds/display-name ::ds/description ::ds/submitted-by])))
(s/def ::ds/OwnedDraftset (s/merge ::ds/DraftsetCommon (s/keys :req-un [::ds/current-owner])))
(s/def ::ds/SubmittedToRole (s/keys :req-un [::ds/claim-role]))
(s/def ::ds/SubmittedToUser (s/keys :req-un [::ds/claim-user]))
(s/def ::ds/SubmittedDraftset (s/and ::ds/DraftsetCommon (s/or :role ::ds/SubmittedToRole :user ::ds/SubmittedToUser)))
(s/def ::ds/Draftset (s/or :owned ::ds/OwnedDraftset :submitted ::ds/SubmittedDraftset))

(def operations #{:delete :edit :submit :publish :claim})
(s/def ::ds/Operation operations)

(s/fdef ds/create-draftset
  :args (s/cat :creator :drafter/EmailAddress :display-name (s/? string?) :description (s/? string?))
  :ret ::ds/OwnedDraftset)

(def ^:private fns-with-specs
  [`ds/create-draftset])

(defn instrument []
  (st/instrument fns-with-specs))

(defn unstrument []
  (st/unstrument fns-with-specs))
