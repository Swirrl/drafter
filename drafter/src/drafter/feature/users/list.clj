(ns drafter.feature.users.list
  (:require [clojure.spec.alpha :as s]
            [drafter.user :as user]
            [drafter.middleware :as middleware]
            [integrant.core :as ig]
            [ring.util.response :as ring]))

(defn get-users-handler
  "Ring handler that returns a list of user objects representing users
  within the system."
  [{user-repo ::user/repo wrap-authenticate :wrap-authenticate}]
  (middleware/wrap-authorize wrap-authenticate :drafter:user:view
   (fn [r]
     (let [users (user/get-all-users user-repo)
           summaries (map user/get-summary users)]
       (ring/response summaries)))))

(defmethod ig/pre-init-spec ::get-users-handler [_]
  (s/keys :req [::user/repo]))

(defmethod ig/init-key ::get-users-handler [_ opts]
  (get-users-handler opts))

