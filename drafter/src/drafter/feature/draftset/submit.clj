(ns drafter.feature.draftset.submit
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.responses :refer [unprocessable-entity-response]]
            [drafter.user :as user]
            [integrant.core :as ig]))

(defn handle-user
  [{:keys [backend] :as resources} repo user draftset-id owner]
  (if-let [target-user (user/find-user-by-username repo user)]
    (feat-common/run-sync
     resources
     (:email owner)
     'submit-draftset-to-user
     draftset-id
     #(dsops/submit-draftset-to-user! backend draftset-id owner target-user)
     #(feat-common/draftset-sync-write-response % backend draftset-id))
    (unprocessable-entity-response (str "User: " user " not found"))))

(defn handle-role
  [{:keys [backend] :as resources} role draftset-id owner]
  (let [role-kw (keyword role)]
    (if (user/is-known-role? role-kw)
      (feat-common/run-sync
       resources
       (:email owner)
       'submit-draftset-to-role
       draftset-id
       #(dsops/submit-draftset-to-role! backend draftset-id owner role-kw)
       #(feat-common/draftset-sync-write-response % backend draftset-id))
      (unprocessable-entity-response (str "Invalid role: " role)))))

(defn handler
  [{:keys [:drafter/backend :drafter/global-writes-lock :drafter.user/repo
           wrap-as-draftset-owner timeout-fn]}]
  (let [resources {:backend backend :global-writes-lock global-writes-lock}]
    (wrap-as-draftset-owner
     (fn [{{:keys [user role draftset-id]} :params owner :identity}]
       (cond
         (and (some? user) (some? role))
         (unprocessable-entity-response
          "Only one of user and role parameters permitted")

         (some? user)
         (handle-user resources repo user draftset-id owner)

         (some? role)
         (handle-role resources role draftset-id owner)

         :else
         (unprocessable-entity-response "user or role parameter required"))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.submit/handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock ::user/repo]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.submit/handler [_ opts]
  (handler opts))
