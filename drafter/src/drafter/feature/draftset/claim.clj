(ns drafter.feature.draftset.claim
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.requests :as req]
            [drafter.responses
             :refer [conflict-detected-response forbidden-response]]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [drafter.async.responses :as response]
            [ring.util.request :as request]))

(defn- respond [result]
  (if (jobutil/failed-job-result? result)
    (response/api-response 500 result)
    (let [[claim-outcome ds-info] (:details result)]
      (if (= :ok claim-outcome)
        (ring/response ds-info)
        (conflict-detected-response "Failed to claim draftset")))))

(defn- handler*
  [{:keys [backend] :as resources}
   {{:keys [draftset-id]} :params user :identity :as request}]
  (if-let [ds-info (dsops/get-draftset-info backend draftset-id)]
    (if (user/can-claim? user ds-info)
      (feat-common/run-sync resources
                            (req/user-id request)
                            'claim-draftset
                            draftset-id
                            #(dsops/claim-draftset! backend draftset-id user)
                            respond)
      (forbidden-response "User not in role for draftset claim"))
    (ring/not-found "Draftset not found")))

(defn handler [{wrap-authenticated :wrap-auth
                :keys [:drafter/backend :drafter/global-writes-lock]}]
  (let [resources {:backend backend :global-writes-lock global-writes-lock}
        inner-handler (partial handler* resources)]
    (-> backend
        (feat-middleware/existing-draftset-handler inner-handler)
        (wrap-authenticated))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.claim/handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-auth]))

(defmethod ig/init-key :drafter.feature.draftset.claim/handler [_ opts]
  (handler opts))
