(ns drafter.feature.users.list
  (:require [clojure.spec.alpha :as s]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]))

(defn get-users-handler
  "Ring handler that returns a list of user objects representing users
  within the system."
  [{wrap-authenticated :wrap-auth 
    user-repo ::user/repo}]
  (wrap-authenticated
   (fn [r]
     (let [users (user/get-all-users user-repo)
           summaries (map user/get-summary users)]
       (ring/response summaries)))))

(defmethod ig/pre-init-spec ::get-users-handler [_]
  (s/keys :req [::user/repo]
          :req-un [::wrap-auth]))

(defmethod ig/init-key ::get-users-handler [_ opts]
  (get-users-handler opts))

