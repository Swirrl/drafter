(ns drafter-client.client-spec
  (:require [clj-time.coerce :refer [from-long]]
            [drafter-client.client :as client]
            [drafter-client.client.impl :as i]
            [drafter.async.spec :as drafter]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
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

(s/def ::AsyncJob (s/and #(instance? AsyncJob %)
                         (s/keys :req-un [::job-id ::restart-id])))

(s/fdef client/job-succeeded? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/job-failed? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/job-complete? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/job-in-progress? :args (s/cat :job-state ::job) :ret boolean?)

(s/fdef client/refresh-job
  :args (s/cat :client any? :access-token any? :job ::AsyncJob)
  :ret ::job)

(s/def ::JobSucceededResult (s/nilable map?))
(s/def ::JobFailedResult client/exception?)
(s/def ::JobResult (s/or :succeeded ::JobSucceededResult :failed ::JobFailedResult))

(s/fdef client/job-status
  :args (s/cat :job ::AsyncJob :job-state ::job)
  :ret (s/or :result ::JobResult :pending #{::pending}))

(s/def ::wait-opts (s/nilable (s/keys :opt [::job-timeout])))

(s/fdef client/wait-result!
  :args (s/alt :arity-3 (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob)
               :arity-4 (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob :opts ::wait-opts))
  :ret ::JobResult)

(s/fdef client/wait-results!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :jobs (s/coll-of ::AsyncJob))
  :ret (s/coll-of ::JobResult))

(s/fdef client/wait!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob)
  :ret ::JobSucceededResult)

(s/fdef client/wait-nil!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :job ::AsyncJob)
  :ret nil?)

(s/fdef client/wait-all!
  :args (s/cat :client ::i/DrafterClient :access-token ::i/AccessToken :jobs (s/coll-of ::AsyncJob))
  :ret (s/coll-of ::JobSucceededResult)
  :fn (fn [{ret :ret {jobs :jobs} :args}] (= (count ret) (count jobs))))
