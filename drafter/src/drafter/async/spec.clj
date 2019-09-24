(ns drafter.async.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as string]
            [clj-time.coerce :refer [from-long]]
            [clj-time.format :refer [formatters parse unparse]]
            [drafter.draftset :as ds])
  (:import [java.util UUID]
           [drafter.draftset DraftsetId]))

(defn promise? [x]
  (and (instance? clojure.lang.IPending x)
       (instance? clojure.lang.IBlockingDeref x)
       (instance? clojure.lang.IFn x)))

(def email-string?
  (s/with-gen (s/and string? #(string/includes? % "@"))
    (fn [] (gen/fmap (fn [[a b]] (str a "@" b ".com"))
                    (gen/tuple gen/string-ascii gen/string-ascii)))))

(def uuid-string?
  (s/with-gen (s/and string? #(try (UUID/fromString %) (catch Throwable _)))
    (fn [] (gen/fmap str (s/gen uuid?)))))

(s/def ::ds/id uuid-string?)

(def draftset-id?
  (s/with-gen (s/and (partial instance? DraftsetId) (s/keys :req-un [::ds/id]))
    (fn [] (gen/fmap (comp ds/->DraftsetId str) (s/gen uuid?)))))

(def date-time-string?
  (s/with-gen (s/and string? #(parse (formatters :date-time) %))
    (fn [] (gen/fmap (comp (partial unparse (formatters :date-time)) from-long)
                    (s/gen int?)))))

(s/def ::id uuid?)
(s/def ::user-id email-string?)
(s/def ::operation symbol?)
(s/def ::status #{:pending :complete})
(s/def ::priority #{:batch-write :background-write :blocking-write :publish-write})
(s/def :internal-job/start-time int?)
(s/def :internal-job/finish-time (s/nilable int?))
(s/def :api-job/start-time date-time-string?)
(s/def :api-job/finish-time (s/nilable date-time-string?))
(s/def ::draftset-id (s/nilable draftset-id?))
(s/def ::draft-graph-id (s/nilable uuid-string?))
(s/def ::function (s/with-gen fn? (fn [] (gen/elements [identity]))))
(s/def ::value-p (s/with-gen promise? (fn [] (gen/elements [(promise)]))))

(s/def ::job
  (s/keys :req-un [::id ::user-id ::operation ::status ::priority
                   :internal-job/start-time :internal-job/finish-time
                   ::draftset-id ::draft-graph-id ::function ::value-p]))

(s/def ::api-job
  (s/keys :req-un [::id ::user-id ::operation ::status ::priority
                   :api-job/start-time :api-job/finish-time]
          :opt-un [::draftset-id ::draft-graph-id]))

(s/def :failed-job-result/type #{:error})
(s/def :success-job-result/type #{:ok})
(s/def :not-found-result/type #{:not-found})
(s/def ::message string?)
(s/def ::error-class string?)
(s/def ::details map?)

(s/def ::failed-job-result
  (s/keys :req-un [:failed-job-result/type ::message ::error-class]
          :opt-un [::details]))

(s/def ::success-job-result
  (s/keys :req-un [:success-job-result/type]
          :opt-un [::details]))

(s/def ::job-result
  (s/or :failed ::failed-job-result :success ::success-job-result))

(s/def ::type #{:ok :error})
(s/def ::error keyword?)
(s/def ::swirrl-object (s/keys :req-un [::type] :opt-un [::details]))
(s/def ::swirrl-error
  (s/merge ::swirrl-object
           (s/keys :req-un [:failed-job-result/type ::error ::message])))

(s/def :http/status int?)
(s/def :ring/headers
  (s/map-of string? (s/or :str string? :list (s/coll-of string?))))

(s/def :ring.json/headers
  (s/and (fn [{:strs [Content-Type]}] (= Content-Type "application/json"))
         :ring/headers))

(s/def :ring/body any?)

(s/def :ring/response
  (s/keys :req-un [:http/status :ring/headers :ring/body]))

(s/def :ring.json/response
  (s/merge :ring/response (s/keys :req-un [:ring.json/headers])))

(s/def :ring-swirrl-error/body ::swirrl-error)
(s/def ::ring-swirrl-error-response
  (s/merge :ring.json/response (s/keys :req-un [:ring-swirrl-error/body])))

(s/def ::ok-object (s/keys :req-un [:success-job-result/type]))

(s/def ::not-found-object (s/keys :req-un [:not-found-result/type ::message]))

(s/def ::finished-job string?)
(s/def ::restart-id uuid?)
(s/def :submitted-job/status #{202})
(s/def :submitted-job/body
  (s/keys :req-un [:success-job-result/type ::finished-job ::restart-id]))
(s/def :submitted-job/response
  (s/merge :ring.json/response
           (s/keys :req-un [:submitted-job/status :submitted-job/body])))

(defn status-result [schema]
  (s/merge schema (s/keys :req-un [::restart-id])))

(s/def ::pending-job-result
  (status-result ::not-found-object))

(defn json-response [schema]
  (s/merge :ring.json/response schema))

(s/def :job-not-finished/body ::pending-job-result)
(s/def :job-not-finished/response
  (json-response (s/keys :req-un [:job-not-finished/body])))

(s/def :not-found/body   (status-result ::not-found-object))
(s/def :failed-job/body  (status-result ::failed-job-result))
(s/def :success-job/body (status-result ::success-job-result))

(s/def :job-status/response
  (s/or :not-found (-> (s/keys :req-un [:not-found/body])
                       (status-result)
                       (json-response))
        :failure   (-> (s/keys :req-un [:failed-job/body])
                       (status-result)
                       (json-response))
        :success   (-> (s/keys :req-un [:success-job/body])
                       (status-result)
                       (json-response))))
