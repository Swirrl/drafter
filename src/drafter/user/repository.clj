(ns drafter.user.repository)

(defprotocol UserRepository
  (find-user-by-username [this username]
    "Attempts to find a user with the given user name in the
    underlying store. Returns nil if no such user was found.")

  (get-all-users [this]
    "Returns all users in this repository"))

(defn get-configured-repository [{:keys [user-repo-ns] :as config}]
  (let [repo-ns-name (symbol user-repo-ns)]
    (require repo-ns-name)
    (if-let [repo-ns (find-ns repo-ns-name)]
      (if-let [get-repo-fn-var (.findInternedVar repo-ns 'get-repository)]
        (@get-repo-fn-var config)
        (throw (RuntimeException. (str "No var get-repository found in namespace " repo-ns-name))))
      (throw (RuntimeException. (str "Cannot find namespace " repo-ns-name))))))
