(ns drafter.feature.draftset.submit
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.responses :refer [unprocessable-entity-response]]
            [drafter.user :as user]
            [integrant.core :as ig]))

(defn handle-user
  [{:keys [backend] :as manager} repo user draftset-id owner]
  (if-let [target-user (user/find-user-by-username repo user)]
    (feat-common/run-sync
      manager
      (:email owner)
      'submit-draftset-to-user
      draftset-id
      #(dsops/submit-draftset-to-user! backend draftset-id owner target-user)
      #(feat-common/draftset-sync-write-response % backend draftset-id))
    (unprocessable-entity-response (str "User: " user " not found"))))

(defn handle-role
  [{:keys [backend] :as manager} role draftset-id owner]
  (let [role-kw (keyword role)]
    (if (user/is-known-role? role-kw)
      (feat-common/run-sync
        manager
        (:email owner)
        'submit-draftset-to-role
        draftset-id
        #(dsops/submit-draftset-to-role! backend draftset-id owner role-kw)
        #(feat-common/draftset-sync-write-response % backend draftset-id))
      (unprocessable-entity-response (str "Invalid role: " role)))))

(defn handler
  [{:keys [:drafter/manager :drafter.user/repo wrap-as-draftset-owner]}]
  (wrap-as-draftset-owner :draft:offer
    (fn [{{:keys [user role draftset-id]} :params owner :identity}]
      (cond
        (and (some? user) (some? role))
        (unprocessable-entity-response
          "Only one of user and role parameters permitted")

        (some? user)
        (handle-user manager repo user draftset-id owner)

        (some? role)
        (handle-role manager role draftset-id owner)

        :else
        (unprocessable-entity-response "user or role parameter required")))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.submit/handler [_]
  (s/keys :req [:drafter/manager ::user/repo]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.submit/handler [_ opts]
  (handler opts))
