(ns drafter.feature.draftset.share
  (:require
   [drafter.backend.draftset.operations :as dsops]
   [drafter.feature.common :as feat-common]
   [drafter.responses :refer [unprocessable-entity-response]]
   [drafter.user :as user]
   [integrant.core :as ig]))

;; TODO unshare

(defn handle-user
  [{:keys [backend] :as manager} repo user draftset-id owner]
  (if-let [target-user (user/find-user-by-username repo user)]
    (feat-common/run-sync
      manager
      (:email owner)
      'share-draftset-with-user
      draftset-id
      #(dsops/share-draftset-with-user! backend draftset-id owner target-user)
      #(feat-common/draftset-sync-write-response % backend draftset-id))
    (unprocessable-entity-response (str "User: " user " not found"))))

(defn handle-permission
  [{:keys [backend] :as manager} permission draftset-id owner]
  (feat-common/run-sync
   manager
   (:email owner)
   'share-draftset-with-permission
   draftset-id
   #(dsops/share-draftset-with-permission! backend
                                           draftset-id
                                           owner
                                           (keyword permission))
   #(feat-common/draftset-sync-write-response % backend draftset-id)))

(defmethod ig/init-key :drafter.feature.draftset.share/handler
  [_ {:keys [drafter/manager drafter.user/repo wrap-as-draftset-owner]}]
  (wrap-as-draftset-owner :draft:share
    (fn [{{:keys [user permission draftset-id]} :params owner :identity}]
      (cond
        (and (some? user) (some? permission))
        (unprocessable-entity-response
         "Only one of user and permission parameters permitted")

        (some? user)
        (handle-user manager repo user draftset-id owner)

        (some? permission)
        (handle-permission manager permission draftset-id owner)

        :else
        (unprocessable-entity-response "user or permission parameter required")))))
