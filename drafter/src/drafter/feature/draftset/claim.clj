(ns drafter.feature.draftset.claim
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.middleware :as middleware]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.requests :as req]
            [drafter.responses
             :refer [conflict-detected-response forbidden-response]]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [drafter.async.responses :as response]))

(defn- respond [result]
  (if (jobutil/failed-job-result? result)
    (response/api-response 500 result)
    (let [[claim-outcome ds-info] (:details result)]
      (if (= :ok claim-outcome)
        (ring/response ds-info)
        (conflict-detected-response "Failed to claim draftset")))))

(defn- handler*
  [{:keys [backend] :as manager}
   {{:keys [draftset-id]} :params user :identity :as request}]
  (if-let [ds-info (dsops/get-draftset-info backend draftset-id)]
    (if (user/can-claim? user ds-info)
      (feat-common/run-sync manager
                            (req/user-id request)
                            'claim-draftset
                            draftset-id
                            #(dsops/claim-draftset! backend draftset-id user)
                            respond)
      (forbidden-response "User not in role for draftset claim"))
    (ring/not-found "Draftset not found")))

(defn handler [{{:keys [backend] :as manager} :drafter/manager}]
  (let [inner-handler (partial handler* manager)]
    (->> inner-handler
        (feat-middleware/existing-draftset-handler backend)
        (middleware/wrap-authorize :editor))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.claim/handler [_]
  (s/keys :req [:drafter/manager]))

(defmethod ig/init-key :drafter.feature.draftset.claim/handler [_ opts]
  (handler opts))
