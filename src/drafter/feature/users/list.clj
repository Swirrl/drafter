(ns drafter.feature.users.list
  (:require [clojure.spec.alpha :as s]
            [drafter.user :as user]
            [integrant.core :as ig]
            [reitit.coercion.spec :as spec]
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

(s/def ::x int?)
(s/def ::y int?)
(s/def ::default-graph-uri (s/coll-of uri?))
(s/def ::total int?)
(s/def ::query-params (s/keys :req-un [::default-graph-uri]))
(s/def ::response (s/keys :req-un [::total]))

(defn get-users-resource [opts]
  {:get {:summary "Gets all users"
         :description "Returns a JSON document containing an array of summary
                       documents, one for each known user."
         :coercion spec/coercion
         :tags ["Users"]
         :swagger {:security {:jws-auth []}
                   :operationId :get-users}
         :parameters {:query ::query-params}
         :responses {"200" {:description "Users found."
                            :body ::response}}
         :handler (get-users-handler opts)}})

(defmethod ig/pre-init-spec ::get-users-resource [_]
  (s/keys :req [::user/repo]
          :req-un [::wrap-auth]))

(defmethod ig/init-key ::get-users-resource [_ opts]
  (get-users-resource opts))
