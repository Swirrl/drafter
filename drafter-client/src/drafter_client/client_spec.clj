(ns drafter-client.client-spec
  (:require [clj-time.coerce :refer [from-long]]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [drafter-client.client :as client]
            [drafter-client.client.impl :as i]
            [drafter-client.client.protocols :as dcpr]
            [drafter-client.client.repo :as repo]
            [drafter.async.spec :as drafter]
            [drafter-client.client.spec :as dcs]
            [drafter-client.client.draftset :as draftset])
  (:import drafter_client.client.AsyncJob))

(def date-time?
  (s/with-gen (partial instance? org.joda.time.DateTime)
    (fn [] (gen/fmap from-long (s/gen int?)))))

(s/def ::draftset-id (s/nilable uuid?))
(s/def ::start-time date-time?)
(s/def ::finish-time (s/nilable date-time?))

(s/def ::job
  (s/keys :req-un [::drafter/id ::drafter/user-id ::drafter/status
                   ::drafter/priority
                   ::start-time ::finish-time]
          :opt-un [::draftset-id ::drafter/draft-graph-id ::drafter/metadata]))


(s/def ::type #{"ok" "error" "not-found"})
(s/def :ok/type #{"ok"})
(s/def :error/type #{"error"})
(s/def :not-found/type #{"not-found"})
(s/def ::details map?)
(s/def ::job-id uuid?)
(s/def ::restart-id uuid?)
(s/def ::message string?)
(s/def ::error-class string?)
(s/def ::job-timeout (s/or :finite integer?
                           :infinite #{##Inf}))

(s/def ::AsyncJob (s/keys :req-un [::job-id ::restart-id]))

(s/fdef client/job-succeeded? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/job-failed? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/job-complete? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/job-in-progress? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/refresh-job
  :args (s/cat :client any? :access-token any? :job ::AsyncJob)
  :ret (s/nilable ::job))

(s/def ::JobSucceededResult (s/nilable map?))
(s/def ::JobFailedResult client/exception?)
(s/def ::JobResult (s/or :succeeded ::JobSucceededResult :failed ::JobFailedResult))

(s/fdef client/job-status
  :args (s/cat :job ::AsyncJob :job-state ::job)
  :ret (s/or :result ::JobResult :pending #{::client/pending}))

(s/def ::wait-opts (s/nilable (s/keys :opt [::job-timeout])))

(s/def ::DrafterClient dcpr/drafter-client?)
(s/def ::AccessToken string?)
(s/def ::MaybeAccessToken (s/nilable ::AccessToken)) ;; when access-token is nil client should have an :auth-provider

(s/fdef client/wait-result!
  :args (s/alt :arity-3 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :job ::AsyncJob)
               :arity-4 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :job ::AsyncJob :opts ::wait-opts))
  :ret ::JobResult)

(s/fdef client/wait-results!
  :args (s/cat :client ::i/DrafterClient :access-token ::MaybeAccessToken :jobs (s/coll-of ::AsyncJob))
  :ret (s/coll-of ::JobResult))

(s/fdef client/wait!
  :args (s/alt :arity-3 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :job ::AsyncJob)
               :arity-4 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :job ::AsyncJob :opts ::wait-opts))
  :ret ::JobSucceededResult)

(s/fdef client/wait-nil!
  :args (s/alt :arity-3 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :job ::AsyncJob)
               :arity-4 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :job ::AsyncJob :opts ::wait-opts))
  :ret nil?)

(s/fdef client/wait-all!
  :args (s/alt :arity-3 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :jobs (s/coll-of ::AsyncJob))
               :arity-4 (s/cat :client ::DrafterClient :access-token ::MaybeAccessToken :jobs (s/coll-of ::AsyncJob) :opts ::wait-opts))
  :ret (s/coll-of ::JobSucceededResult)
  :fn (fn [{ret :ret {jobs :jobs} :args}] (= (count ret) (count jobs))))






(alias 'endpoint (create-ns 'drafter-client.endpoint))

(s/def :drafter-client/endpoint (s/or :live draftset/live? :draft draftset/draft?))

(alias 'user (create-ns 'drafter-client.user))

(s/def ::user/role string?)
(s/def ::user/email string?)

(s/def :drafter-client/user (s/keys :opt-un [::role ::email])) ;; as returned by pmd3/basic-auth drafter

(s/fdef client/get-users
  :ret (s/* :drafter-client/user))

(s/def :drafter-client.triplestore/params (s/map-of keyword? any?))

(s/fdef repo/make-repo
  :args (s/cat :client ::DrafterClient
               :endpoint :drafter-client/endpoint
               :token ::MaybeAccessToken
               :params :drafter-client.triplestore/params)
  :ret (partial instance? org.eclipse.rdf4j.repository.Repository))
