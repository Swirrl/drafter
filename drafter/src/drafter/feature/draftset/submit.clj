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

(defn handle-permission
  [{:keys [backend] :as manager} permission draftset-id owner]
  (feat-common/run-sync
   manager
   (:email owner)
   'submit-draftset-to-permission
   draftset-id
   #(dsops/submit-draftset-to-permission! backend
                                          draftset-id
                                          owner
                                          (keyword permission))
   #(feat-common/draftset-sync-write-response % backend draftset-id)))

;; Maps a role to a permission that that role has, but less privileged roles
;; don't have.
(def role->canonical-permission
  {"editor" "draft:edit" "publisher" "draft:publish"})

(defn handler
  [{:keys [:drafter/manager :drafter.user/repo wrap-as-draftset-owner]}]
  (wrap-as-draftset-owner :draft:submit
    (fn [{{:keys [user permission role draftset-id]} :params owner :identity}]
      ;; The role parameter is deprecated
      (let [permission (or permission (role->canonical-permission role))]
        (cond
          (and (some? user) (some? permission))
          (unprocessable-entity-response
           "Only one of user and permission parameters permitted")

          (some? user)
          (handle-user manager repo user draftset-id owner)

          (some? permission)
          (handle-permission manager permission draftset-id owner)

          :else
          (unprocessable-entity-response "user or permission parameter required"))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.submit/handler [_]
  (s/keys :req [:drafter/manager ::user/repo]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.submit/handler [_ opts]
  (handler opts))
